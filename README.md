# Nexa — Hito 1

Hito 1 demuestra únicamente que Spark arranca y ejecuta una acción real. El
servicio `spark-smoke-test` crea una `SparkSession`, ejecuta
`spark.range(1).count()` y termina en `local[2]`.

## Imagen, contenedor y Compose

- La imagen oficial de Apache Spark es el paquete reutilizable:
  `apache/spark:3.5.8-scala2.12-java17-python3-ubuntu`.
- El contenedor es una ejecución de esa imagen.
- Docker Compose es la receta que configura el servicio, el montaje del script
  y su red.

## Versiones

| Componente | Versión |
|---|---|
| Apache Spark | 3.5.8 |
| Scala | 2.12 |
| Java | 17.x |
| Python | 3.10.x |
| Delta Lake | 3.3.0 como baseline futuro; no instalado en Hito 1 |

`local[2]` ejecuta Spark localmente con dos threads de trabajo. No levanta un
cluster ni requiere un master o workers.

## Ejecutar

La red `ecosistema-network` es externa a este Compose y debe existir antes de
iniciar el servicio. Créala una sola vez si todavía no existe:

```bash
docker network inspect ecosistema-network >/dev/null 2>&1 || docker network create ecosistema-network
```

La red solo se crea si todavía no existe. Luego valida, obtiene la imagen
exacta y ejecuta el servicio one-shot:

```bash
docker compose config --quiet
docker compose pull spark-smoke-test
docker compose up --abort-on-container-exit --exit-code-from spark-smoke-test
```

La salida exitosa contiene una línea como:

```text
NEXA_SMOKE_TEST_OK spark=3.5.8 python=3.10.x java=17.x master=local[2] count=1
```

`spark` y `python` indican las versiones reales verificadas; `java` confirma la
versión de Java; `master` confirma el modo efectivo `local[2]`; y `count`
confirma que Spark ejecutó el conteo esperado. El contenedor termina por sí
mismo con código `0`.

## Límites del hito

La red queda declarada para la integración futura, pero Hito 1 todavía no
consume Redpanda ni lee eventos. Tampoco instala Delta Lake ni crea tablas
Bronze/Silver, checkpoints, DuckDB o FastAPI. Ratchet y su Compose no se
modifican. La lectura del contrato de eventos desde Redpanda pertenece al
Hito 2.
