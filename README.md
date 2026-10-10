# go-sqlite-multiwriter

Varios escritores concurrentes sobre **una** base de datos SQLite, en **Go puro** (sin cgo).

Es un port a Go del motor de [sqliteai/sqlite-multiwriter](https://github.com/sqliteai/sqlite-multiwriter)
(artículo: [We solved SQLite's single-writer limitation](https://marcobambini.substack.com/p/we-solved-sqlites-single-writer-limitation)).
Igual que el original, es un **VFS**: tu SQL, tu esquema y tu fichero `.db` no cambian, y el fichero sigue
siendo una base SQLite normal cuando se cierra la última conexión.

Funciona sobre [`modernc.org/sqlite`](https://gitlab.com/cznic/sqlite) (SQLite traducido a Go), así que se
compila con `CGO_ENABLED=0` para cualquier plataforma que soporte modernc.

```go
import (
    multiwriter "github.com/digiogithub/go-sqlite-multiwriter"
)

db, err := multiwriter.Open("app.db", multiwriter.Options{}) // *sql.DB, cada conexión del pool es un escritor
...
err = multiwriter.RunTx(ctx, db, nil, func(tx *sql.Tx) error {
    _, err := tx.Exec("UPDATE account SET balance = balance + 10 WHERE id = ?", 7)
    return err
}) // reintenta la transacción entera si pierde contra otro commit (SQLITE_BUSY_SNAPSHOT)
```

También se puede usar directamente con el driver de modernc: basta importar el paquete (registra el VFS) y
abrir con la URI `file:app.db?vfs=multiwriter`.

## Cómo funciona

```
escritor 1 --> WAL privado --+
escritor 2 --> WAL privado --+--> validación y publicación --> log de commits --> fichero .db
escritor 3 --> WAL privado --+    (gana el primero en confirmar)                 (compactación)
```

* **Carriles privados.** Cada conexión tiene su propio WAL en memoria y su propio wal-index: SQLite cree que es
  el único escritor, así que nadie espera por el lock de escritura del WAL. Las escrituras al fichero principal
  se descartan; sólo el compactador escribe el `.db`.
* **Snapshots.** Cada transacción lee una instantánea (época). Las páginas confirmadas se guardan en un almacén
  de versiones `(página, época)` con locks por franjas; una lectura toma la versión más nueva ≤ su snapshot, o
  la página del fichero.
* **Commit.** Al llegar el frame de commit se valida: (1) el *schema cookie* no cambió; (2) ninguna página que
  la transacción **leyó** tiene una versión nueva (evita *write skew*; una página interior de tabla cuyo
  enrutado no cambió se perdona); (3) ninguna página que **escribió** tiene una versión nueva. Si falla, la
  transacción se rechaza con `SQLITE_BUSY_SNAPSHOT` y la aplicación la repite.
* **Reubicación.** Si el único conflicto es que otros commits también hicieron crecer el fichero (inserciones
  concurrentes), las páginas nuevas se renumeran por encima del final actual, se reescriben las referencias y
  se fusiona la página 1.
* **Fusión de páginas interiores.** Dos transacciones que dividen hojas distintas del mismo padre se fusionan a
  tres bandas sobre el padre.
* **Durabilidad.** Cada commit se escribe en un log segmentado (`<db>-mw`, `<db>-mw.000001`...) con *group
  commit* (`synchronous=FULL` por defecto: el commit está en disco cuando vuelve). El log se compacta en el
  `.db` en segundo plano. Tras un crash, la recuperación reproduce el prefijo válido del log.
* **Barrera de esquema.** Un `CREATE/DROP/ALTER/...` (detectado con `sqlite3_trace_v2`) espera a que terminen
  los escritores en vuelo y bloquea nuevos mientras corre.

Aislamiento: *snapshot isolation* con *first-committer-wins* por página más validación de las páginas leídas.

## Estado del port

| Funcionalidad del original | Estado |
|---|---|
| Carriles privados (WAL + wal-index privados por conexión) | ✅ |
| Almacén de versiones, snapshots, validación lectura/escritura/esquema | ✅ |
| Reubicación de páginas nuevas, fusión de páginas interiores, atajo de enrutado | ✅ |
| Log durable con group commit, recuperación tras crash, compactación, GC | ✅ |
| Barrera DDL, turno para zonas calientes, pragmas no soportados rechazados | ✅ |
| **Rebase** (`mw_rebase=1`: re-ejecutar cambios de fila cuando el conflicto es sólo de página) | ⏳ fase 2 |
| **Multiproceso** (`mw_mp=1`: índice compartido y log en memoria compartida) | ⏳ fase 3 (hoy: un proceso por base; un segundo proceso recibe `SQLITE_BUSY`) |

Los ficheros `-mw*` no son compatibles en formato con los del original en C (no hace falta: sólo existen
mientras la base está abierta; al cerrar la última conexión todo queda en el `.db`).

## Rendimiento

`go run ./cmd/mwbench -workload bulk -writers 1,4,16` — máquina de 4 núcleos, disco, `synchronous=FULL`, ambos
con modernc (SQLite de serie en WAL con `BEGIN IMMEDIATE` y `busy_timeout`):

| bulk (100 inserts/tx, cada escritor su tabla) | SQLite tx/s | multiwriter tx/s | SQLite p99.9 | multiwriter p99.9 |
|---:|---:|---:|---:|---:|
| 1 escritor | 1262 | 1060 | 7 ms | 4 ms |
| 4 escritores | 1068 | **1902** | 1033 ms | **15 ms** |
| 16 escritores | 1054 | **1814** | 4036 ms | **39 ms** |

Cuando todos los escritores tocan **la misma página** (filas pequeñas vecinas, contadores calientes) el
rendimiento es similar al de SQLite pero con reintentos; ese es el caso que resuelve el *rebase* (fase 2).

## Opciones (URI)

| Parámetro | Significado |
|---|---|
| `vfs=multiwriter` | activa el motor (`mw=0` lo desactiva para esa base) |
| `mw_readcheck=0` | no validar las páginas leídas: menos reintentos, pero permite *write skew* |
| `mw_log_max_mb` | tamaño del log antes de compactar (32 MB) |
| `mw_base_cache_mb` | caché de páginas del fichero en memoria (64 MB) |
| `mw_gc` | commits entre dos recolecciones de versiones (64) |
| `mw_noreloc`, `mw_nomerge`, `mw_noroute` | desactivan reubicación / fusión / atajo de enrutado (medición) |

## Límites

* Sólo WAL; no `locking_mode=EXCLUSIVE` ni `auto_vacuum`. No sobre sistemas de ficheros de red.
* Mientras está abierta, la base sólo debe usarse a través del motor (el log `-mw*` tiene commits que aún no
  están en el `.db`).
* Una transacción que falla con `SQLITE_BUSY_SNAPSHOT` debe repetirse **entera** (`RunTx` lo hace).
* Un lector que nunca termina impide compactar (igual que en el original).

## Tests

```
go test ./...            # concurrencia, invariantes, write skew, DDL, crash y recuperación
go test -race ./...
go run ./cmd/mwbench -workload bulk|groups|hot -writers 1,4,16
```

Licencia Apache 2.0 (ver `LICENSE` y `NOTICE`).
