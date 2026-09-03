# Bitácora de aprendizaje — Nexa

## Cómo usar este documento

Esta es una bitácora acumulativa, escrita para poder volver a entender Nexa
meses después y para que los chats de los próximos hitos puedan continuarla.
Cada hito debe agregar su propia sección. No se debe borrar una explicación
anterior solo porque después aparezca una decisión más técnica: primero queda
registrado qué entendí y luego qué se implementó.

Este es el único documento interno versionado para aprendizaje y decisiones de
los hitos. No se crearán especificaciones o planes paralelos que repitan su
contenido; el uso y la operación del proyecto permanecen en `README.md`, y el
código y sus pruebas son la fuente de verdad de la implementación.

## Estado al 2 de septiembre de 2026

- Nexa ya tiene la infraestructura mínima del Hito 1; todavía no tiene código
  de negocio.
- El Hito 1 tuvo su primer checkpoint conceptual: Docker, Compose, Redpanda,
  redes Docker, puertos y el rol de Spark.
- El diseño formal del Hito 1 fue aprobado y su implementación quedó verificada
  con la imagen oficial de Spark.
- El Hito 2 todavía no comenzó.
- No se modificó Proyecto_Nexa.md: es privado, está ignorado localmente y no
  debe copiarse ni publicarse.

La diferencia es importante: las secciones iniciales documentan aprendizaje y
verificaciones previas; la sección 13 registra la infraestructura que sí fue
implementada y verificada.

## 1. De dónde partía

Yo ya había usado Docker Compose en otros proyectos, pero conocía más la
práctica que el modelo mental completo. Tendía a mezclar estas ideas:

- imagen Docker;
- contenedor;
- Docker Compose;
- máquina virtual;
- puertos y endpoints;
- comunicación entre servicios.

También entendía que Redpanda estaba relacionado con los eventos de Ratchet,
pero lo describía como un “observador” o notificador. No tenía claro que Spark
es el motor que procesa los datos, ni cómo ese aprendizaje se relaciona con
Databricks.

La conversación sirvió para separar esas capas antes de escribir código. Ese
orden es parte del objetivo del proyecto: no agregar infraestructura que luego
no pueda explicar.

## 2. Modelo mental de Docker

### Imagen

Una imagen es un paquete reutilizable que contiene una aplicación, sus
dependencias y el entorno de usuario que necesita para ejecutarse. Es parecido
a una plantilla preparada, no a un programa que ya está corriendo.

No debo pensarla como una máquina virtual completa. Un contenedor comparte el
kernel del sistema anfitrión, aunque mantiene aislados su proceso, filesystem,
red y dependencias. Docker da portabilidad y aislamiento, pero no es lo mismo
que tener otra computadora completa.

### Contenedor

Un contenedor es una imagen en ejecución. Si una misma imagen se ejecuta dos
veces, pueden existir dos contenedores separados, cada uno con su propio
proceso y estado de ejecución. Lo que ocurre dentro de un contenedor no cambia
la imagen base.

La “entrada” hacia un servicio no es el contenedor: normalmente es un puerto.
Por ejemplo, un puerto puede ser la puerta de red por la que una aplicación
habla con Redpanda.

### Docker Compose

Compose es un archivo de configuración y una receta operativa. Describe qué
servicios levantar, qué imágenes usar, qué variables de entorno necesitan,
qué puertos publicar, qué volúmenes persistir y a qué redes conectarlos.

Por eso “Nexa tiene su propio Compose” no significa “Nexa tiene su propia
máquina virtual”. Significa que Nexa tiene una receta independiente para
levantar y configurar su propia infraestructura. Así puede iniciarse,
detenerse y evolucionar sin fusionarse con Ratchet.

## 3. Qué existe hoy en Ratchet

El Compose de infraestructura de Ratchet, ubicado en la carpeta infra, declara
tres servicios de Docker:

1. PostgreSQL: persiste los datos transaccionales.
2. Redis: soporta expiraciones y caché.
3. Redpanda: recibe y distribuye eventos.

Los cuatro microservicios Java —inventory, reservation, payment y
notification— no están definidos en ese Compose de infraestructura. El README
indica que se ejecutan aparte mediante Maven o sus JARs.

En la comprobación realizada durante esta conversación, docker ps no mostró
contenedores activos. Eso describe el estado de Docker en ese momento; no
significa que el Compose no pueda levantarlos.

## 4. Qué hace Redpanda

Redpanda es un intermediario y registro de eventos compatible con la API de
Kafka. No sabe automáticamente qué ocurre dentro de Ratchet y no decide si
una reserva es válida.

Ratchet decide qué hechos del negocio quiere publicar. El flujo es:

    Ratchet ejecuta una operación
            ↓
    guarda un evento en su Outbox de PostgreSQL
            ↓
    el publisher lo envía a Redpanda
            ↓
    Redpanda conserva el mensaje y lo entrega a consumidores

Por ejemplo, un mensaje puede decir que se creó o confirmó una reserva. El
tópico canónico es reservation.events.v1. notification-service y Nexa pueden
consumir el mismo tópico de forma independiente.

Redpanda no reemplaza PostgreSQL. PostgreSQL guarda el estado transaccional de
Ratchet; Redpanda transporta y conserva eventos para que otros componentes
reaccionen a hechos ya publicados.

Una forma más precisa de expresarlo es:

> Redpanda no es el observador de Ratchet. Es el lugar donde Ratchet publica
> eventos y desde donde los consumidores pueden leerlos.

## 5. La red Docker compartida

Una red Docker compartida se parece a una LAN privada entre contenedores que
están conectados a ella:

    Redpanda de Ratchet ─── red Docker compartida ─── Spark de Nexa

No conecta imágenes ni máquinas virtuales. Conecta contenedores ejecutándose y
les permite resolverse por nombre y comunicarse por TCP.

Tampoco significa que Nexa pueda acceder automáticamente a PostgreSQL, Redis
o todos los microservicios de Ratchet. La intención es conectar Spark con
Redpanda, y mantener el resto fuera del alcance de Nexa.

La red compartida no debe describirse como un canal cifrado. Es una forma de
conectividad y aislamiento de red local, no una solución completa de seguridad
inter-servicio.

## 6. Puertos del host y nombres internos

Ratchet configura dos formas de llegar al mismo Redpanda:

- localhost:9092: lo usa un programa ejecutándose directamente en la
  computadora anfitriona.
- redpanda:29092: lo usan contenedores conectados a la red Docker.

Dentro de un contenedor, localhost significa “este mismo contenedor”. No
significa la computadora anfitriona ni otro contenedor. Por eso Spark no debe
usar localhost para buscar a Redpanda.

La diferencia entre 9092 y 29092 no implica necesariamente dos Redpandas. Son
listeners distintos para contextos de red distintos: uno para clientes del
host y otro para clientes internos de Docker.

## 7. Qué hará Spark en Nexa

Spark es el motor de procesamiento. No es el broker de eventos, no es la base
de datos y no es el responsable de coordinar los contenedores.

En el Hito 1 solo se debe demostrar que Spark inicia correctamente y que una
prueba mínima puede confirmar que está operativo.

En el Hito 2, el flujo será:

    Redpanda
       ↓ eventos JSON
    Spark Structured Streaming
       ↓ procesamiento incremental
    Delta Lake bronze
       ↓ más adelante
    Silver y capas posteriores

Spark no recibirá una “marea de datos random”. Recibirá eventos estructurados
según un contrato, por ejemplo RESERVATION_CONFIRMED, con campos como eventId,
resourceId, occurredAt y payload.

En Hito 2, Spark conservará el JSON original y lo guardará en bronze junto con
las columnas técnicas que se aprueben. No lo transformará todavía en silver.

### Qué significa “incrementalmente”

No significa que cada evento se procese una sola vez para siempre, ni que los
datos nunca puedan repetirse. Significa que Spark mantiene una consulta
continua y va procesando los nuevos registros que aparecen desde el último
offset o checkpoint conocido, normalmente en pequeños micro-batches.

En vez de volver a leer todo el tópico cada vez que llega un evento, procesa
el tramo nuevo y conserva solo el estado necesario para continuar. Los
checkpoints y la entrega at-least-once se estudiarán con más profundidad antes
del Hito 2.

## 8. Por qué Spark sirve para aprender Databricks

Databricks es una plataforma gestionada; no es simplemente “otro nombre para
Spark”. Databricks proporciona la plataforma, los recursos de cómputo, la
gestión de jobs y otras capacidades. Apache Spark es el motor de procesamiento
que se usa dentro de ese ecosistema, y Delta Lake es una de sus capas de
almacenamiento principales.

Por eso Nexa practica localmente conceptos transferibles a un trabajo con
Databricks:

- DataFrames y transformaciones;
- lectura de fuentes de eventos;
- Structured Streaming;
- offsets y checkpoints;
- escritura en Delta Lake;
- posteriormente, agregaciones y arquitectura bronze/silver/gold.

La meta no es afirmar que se usó Databricks. La afirmación correcta sería que
se trabajó con Apache Spark Structured Streaming y Delta Lake de forma
self-hosted, usando tecnologías base del ecosistema de Databricks.

## 9. Flujo completo que puedo explicar ahora

    Ratchet crea o cambia una reserva
            ↓
    Outbox de Ratchet registra el evento
            ↓
    Ratchet publica JSON en Redpanda
            ↓
    Redpanda conserva el evento en reservation.events.v1
            ↓
    Spark de Nexa lo lee como consumidor
            ↓
    Delta bronze conserva la evidencia cruda
            ↓
    Más adelante Spark transforma bronze en silver

Nexa es un consumidor puro: no llama a Ratchet, no escribe en su PostgreSQL o
Redis y no publica cambios hacia Ratchet o Cypher.

## 10. Verificaciones realizadas

- Proyecto_Nexa.md fue leído completo y no se modificó.
- El repositorio Nexa estaba prácticamente vacío y sin commits propios.
- No se encontraron instrucciones locales adicionales.
- Ratchet tenía cambios locales no relacionados; se dejaron intactos.
- El commit cb83df2 está en HEAD y en origin/main.
- Ese commit implementa el contrato de eventos en reservation.events.v1.
- El contrato usa JSON, message key resourceId y entrega at-least-once.
- Están implementados los cinco tipos canónicos: hold creado, confirmado,
  liberado, expirado y rechazado.
- DOUBLE_BOOKING_BLOCKED no forma parte de ese commit.

## 11. Lo que todavía no está implementado

El checkpoint conceptual y técnico específico del Hito 2 sigue pendiente:

- tópico;
- partición;
- message key;
- offset;
- consumer group;
- checkpoint de Spark;
- entrega at-least-once;
- duplicado de entrega frente a evento de negocio distinto;
- diferencia entre eventId y holdId.

## 12. Flujo recomendado para los próximos chats

Cada chat de un nuevo hito debería:

1. leer esta bitácora y Proyecto_Nexa.md;
2. agregar una sección con lo que yo entendí antes de implementar;
3. separar conceptos aprendidos, decisiones aprobadas y código real;
4. actualizar el estado solo después de verificar pruebas;
5. registrar qué quedó fuera de alcance;
6. no declarar terminado un hito solo porque exista un archivo o un contenedor.

La bitácora vive dentro del repositorio para que Codex pueda leerla y
actualizarla desde futuros chats que trabajen sobre el mismo workspace. La
documentación oficial de OpenAI también describe que los proyectos pueden
agrupar chats, archivos e instrucciones relacionados; en este caso, el archivo
versionado dentro de Nexa es la fuente más directa y auditable para compartir
el contexto técnico entre tareas.

## 13. Cierre verificado del Hito 1

Se implementó la infraestructura mínima aprobada: `docker-compose.yml` tiene
un único servicio one-shot, basado directamente en
`apache/spark:3.5.8-scala2.12-java17-python3-ubuntu`, con el script montado en
modo read-only y conectado a la red externa `ecosistema-network`.

La ejecución real creó una `SparkSession` en `local[2]` y comprobó que
`spark.range(1).count()` devuelve `1`. La salida observada fue:

    NEXA_SMOKE_TEST_OK spark=3.5.8 python=3.10.12 java=17.0.17 master=local[2] count=1

El pull de la imagen exacta terminó correctamente y Compose terminó con código
`0`. El contenedor quedó detenido como corresponde a un servicio one-shot.
`ecosistema-network` quedó declarada como externa y disponible, pero Hito 1 no
consume Redpanda ni lee eventos.

Quedaron fuera Kafka/Redpanda consumers, Delta Lake instalado, Bronze/Silver,
checkpoints, DuckDB, FastAPI y cualquier cambio en Ratchet. El siguiente límite
pedagógico es Hito 2: explicar cómo leer el contrato de eventos desde Redpanda,
sin adelantar todavía su implementación.

## 14. Cierre personal

Antes veía Docker como una especie de máquina virtual empaquetada. Ahora lo
entiendo como un sistema para empaquetar y ejecutar procesos aislados: la
imagen es el paquete, el contenedor es el paquete corriendo y Compose es la
receta para levantar y conectar esos contenedores.

Antes describía Redpanda como un observador. Ahora sé que Ratchet publica allí
eventos concretos y que los consumidores los leen sin acceder al estado interno
de Ratchet.

Ahora puedo ubicar a Spark en la arquitectura: no observa ni coordina; procesa
los eventos que consume y, en Hito 2, conserva una copia cruda en Delta bronze.
Más adelante será la herramienta para transformar esa evidencia en datos
curados. Esa es la parte que quiero aprender porque conecta directamente con
el trabajo de Data Engineering y con el uso de Spark dentro de Databricks.
