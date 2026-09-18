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

## 15. Hito 2 — aprendizaje, decisiones y verificación

### 15.1 Cómo se construyó el aprendizaje

Este hito se trabajó de forma interactiva y en partes cortas. No intenté leer
todo de una vez: fui explicando con mis palabras lo que entendía, recibiendo
correcciones y volviendo a explicar los puntos que todavía confundía. La
bitácora debe conservar ese recorrido, incluidas las dudas que ayudaron a
afinar el modelo mental.

Al principio entendía un tópico como un lugar donde Ratchet registra sus
transacciones, y pensaba que una `resourceId` mandaba cada recurso a su propia
partición. La corrección importante fue que la key se usa para decidir la
partición, pero muchos recursos pueden compartir una misma partición; lo que
se conserva es el orden dentro de esa partición. Los offsets son posiciones
incrementales asignadas por el broker dentro de cada partición: distintos
recursos pueden quedar intercalados, por ejemplo `a1, b1, a2`, sin perder el
orden de cada partición.

También pregunté si Redpanda necesitaba guardar todo en memoria y por qué no
se agotaba la RAM. La respuesta que me quedó es que Redpanda persiste el log y
usa memoria acotada para operar; los consumidores no reciben todo de una vez.
Spark consulta y procesa tramos limitados en micro-batches. Si los productores
van más rápido, puede crecer el retraso del consumidor, pero no existe un
protocolo mágico que ponga Nexa al día: Spark procesa el backlog según su
capacidad y la configuración de la fuente.

Otro ajuste importante fue separar Kafka de Redpanda. Redpanda es el broker
que Nexa tiene disponible y Kafka es la API/protocolo compatible que permite
que productores y consumidores hablen con él. Spark y notification-service son
consumidores independientes; no deben compartir el mismo consumer group si
ambos necesitan recibir todos los eventos, porque un grupo reparte las
particiones entre sus miembros.

La tabla y el orden jerárquico fueron especialmente útiles para mí. El flujo
conceptual quedó así:

    Ratchet produce hechos de negocio
            ↓
    Redpanda, broker compatible con Kafka
            ↓
    tópico → particiones → registros con key, value y offset
            ↓
    grupos de consumidores independientes
            ↓
    consulta continua de Spark
            ↓
    micro-batches → tabla Delta Bronze

La diferencia entre un evento del negocio y un registro técnico también quedó
más clara. Un hold puede tener varios eventos —creado, confirmado, liberado,
expirado o rechazado—. `eventId` identifica cada evento; `holdId` identifica el
hold al que se refiere. Por eso dos mensajes con el mismo `holdId` y distintos
`eventId` pueden ser dos eventos válidos del ciclo de vida, no un duplicado.
Un duplicado de entrega es volver a leer el mismo registro, identificado por
su posición Kafka y normalmente por su `eventId`.

En Bronze, primero confundí el payload con el evento completo y después
entendí que es un campo dentro del JSON `value`; su contenido depende del tipo
de evento. También confundí Data Lake, Delta Lake y Bronze como niveles
secuenciales de sofisticación. La corrección es que el Data Lake describe el
almacenamiento, Delta Lake aporta el formato y las capacidades de tabla,
transacciones e historial, y Bronze es la primera capa lógica de datos crudos
organizados sobre ese almacenamiento. Bronze conserva el JSON original, no lo
convierte todavía en métricas Silver o Gold.

Finalmente entendí que el checkpoint no es Bronze: Bronze guarda los datos y
el checkpoint guarda el estado operativo de la consulta para poder continuar.
`startingOffsets=latest` sirve para decidir el punto inicial de una consulta
nueva; cuando ya existe checkpoint, Spark recupera desde ese checkpoint y no
vuelve a usar `startingOffsets` para resetearla.

Hay tres situaciones distintas que antes estaba metiendo bajo la palabra
“duplicado”. Primero, Spark puede releer o reintentar el mismo micro-batch si
falla antes de terminar el ciclo de progreso. Segundo, el sink nativo de Delta
coordina su log transaccional con el progreso de Structured Streaming: con la
misma consulta y el mismo checkpoint, el commit del mismo batch es idempotente
y no debería agregar otra vez sus filas. La [documentación oficial de Delta
sobre streaming](https://docs.delta.io/delta-streaming/) describe esta
propiedad de exactly-once del sink nativo.

Tercero, un productor puede publicar dos veces el mismo hecho de negocio. Si
son dos registros Kafka con offsets distintos, Delta recibe dos entradas
distintas y Bronze conserva ambas. Eso no es una relectura del mismo batch y
no se resuelve con el commit idempotente del sink. En este hito no se hace
deduplicación de negocio porque Bronze debe conservar la evidencia cruda. Esta
sesión no forzó una caída exactamente durante un commit, así que la propiedad
del sink está documentada y razonada, no presentada como una prueba local de
una caída.

Para próximos hitos, la forma de explicación que mejor me funcionó fue:

- resumir primero dónde estoy parado en el ciclo completo;
- usar tablas, jerarquías y flujos cuando haya varias capas o relaciones;
- avanzar en bloques cortos y pedirme que reconstruya cada concepto con mis
  palabras;
- corregir explícitamente los errores, incluyendo la diferencia entre palabras
  parecidas como evento, payload, `eventId` y `holdId`;
- incorporar mis dudas reales en el resumen final, para que la bitácora siga
  siendo útil dentro de unos meses.

### 15.2 Decisiones aprobadas

- Construir únicamente `Redpanda → Spark Structured Streaming → Delta Lake
  Bronze`.
- Crear `streaming/bronze_ingest.py` y agregar el servicio `spark-bronze` sin
  retirar ni alterar `spark-smoke-test`.
- Consumir exclusivamente `reservation.events.v1` desde `redpanda:29092`.
- Reutilizar la imagen existente de Spark 3.5.8, Scala 2.12, Java 17, Python
  3.10 y `local[2]`.
- Usar `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8` y
  `io.delta:delta-spark_2.12:3.3.0`. La compatibilidad se contrastó con la
  [matriz oficial de releases de Delta Lake](https://docs.delta.io/releases/),
  la [documentación oficial de integración Kafka de Spark 3.5.8](https://spark.apache.org/docs/3.5.8/structured-streaming-kafka-integration.html)
  y la [compatibilidad Kafka de Redpanda](https://docs.redpanda.com/streaming/current/develop/kafka-clients/).
- Configurar las extensiones de Delta y usar una consulta continua con salida
  `append`.
- Mantener exactamente estas columnas Bronze: `key STRING`, `value STRING`,
  `topic STRING`, `partition INT`, `offset LONG` y `timestamp TIMESTAMP`.
  `value` conserva el JSON completo como texto; no se extraen campos de
  negocio ni se agrega una columna técnica adicional.
- Persistir `data/bronze` y `data/checkpoint/bronze` mediante bind mounts
  locales. `data/` queda ignorado por Git.
- Preparar esos directorios con un servicio one-shot que corre como root solo
  durante la creación y ajuste de permisos; el consumer de Spark continúa con
  su usuario normal UID 185 y no se modifican ni reinicializan los datos
  existentes.
- Usar `startingOffsets=latest` en una consulta sin checkpoint y dejar que el
  checkpoint sea la fuente de recuperación en los reinicios.
- No implementar deduplicación de negocio, Silver, ventanas, watermark, HLL,
  anomalías, DuckDB, LLM, FastAPI, Cypher ni cambios en Ratchet.
- No afirmar exactly-once global ni unicidad de negocio; distinguir la
  relectura de un batch, el commit idempotente del sink nativo Delta y dos
  registros Kafka distintos con offsets distintos.

### 15.3 Implementación y ajustes encontrados

La implementación se mantuvo mínima. El job crea la fuente Kafka, proyecta los
seis campos aprobados, escribe en Delta con modo append y espera continuamente
con `awaitTermination()`. Registra el topic, broker, rutas de Bronze y
checkpoint al iniciar, y registra la excepción si la consulta termina con
error.

La primera ejecución real reveló un problema de entorno que no había que
ocultar: la imagen ejecuta como el usuario `spark`, cuyo `HOME` es
`/nonexistent`, y Ivy no podía escribir su caché al resolver `--packages`. La
corrección mínima fue configurar `spark.jars.ivy=/tmp/.ivy2`; no se agregó una
dependencia ni una abstracción para resolverlo.

La revisión posterior encontró el mismo tipo de problema en el bind mount: en
un checkout sin `data/`, Docker crea el directorio como `root:root` y UID 185
no puede crear el checkpoint. La corrección fue agregar `spark-bronze-init`, un
servicio one-shot que corre como root solo para crear
`data/bronze` y `data/checkpoint/bronze`, cambiar el propietario de esos
directorios a `185:185` y establecer `0755`. No ejecuta Spark como root, no usa
`chmod 777`, no borra archivos y no aplica `chown -R` al contenido existente.

### 15.4 Evidencia de verificación

- `docker compose config --quiet` terminó correctamente después del cambio.
- `docker compose pull spark-smoke-test` terminó correctamente.
- El smoke test del Hito 1 volvió a pasar con:

      NEXA_SMOKE_TEST_OK spark=3.5.8 python=3.10.12 java=17.0.17 master=local[2] count=1

- Se levantó Redpanda desde el Compose de infraestructura de Ratchet y se
  conectó su contenedor existente a `ecosistema-network` sin modificar ese
  repositorio. `rpk cluster info` confirmó el broker `redpanda:29092`.
- Se creó `reservation.events.v1` con una partición para la prueba controlada.
  La publicación de tres eventos produjo `partition=0 offset=0`, `1` y `2`.
- `spark-bronze` quedó en estado `Up`, resolvió y cargó los artefactos Kafka y
  Delta, y registró el arranque con el topic, broker y rutas esperadas.
- El fallo de permisos se reprodujo en un checkout temporal sin `data/`: el
  contenedor terminó con `java.io.IOException: mkdir of
  file:/opt/nexa/data/checkpoint/bronze failed` y el directorio era `root:root`.
  Luego de agregar el inicializador, los mismos pasos públicos del README
  crearon los directorios, el inicializador terminó con código `0` y
  `spark-bronze` arrancó con su usuario normal `spark` (UID 185).
- Sobre el workspace existente, el inicializador terminó con código `0` y los
  hashes de los logs Delta y offsets del checkpoint se mantuvieron sin cambios.
- La consulta Delta devolvió estas filas, verificadas por
  `topic + partition + offset`:

      reservation.events.v1  0  0  resource-h2-1  nexa-h2-event-1
      reservation.events.v1  0  1  resource-h2-1  nexa-h2-event-2
      reservation.events.v1  0  2  resource-h2-2  nexa-h2-event-3

  El campo `value` conservó el JSON completo, incluidos `eventId`,
  `eventType`, `resourceId`, `holderRef` y `payload`.
  La lectura final incluyó también `timestamp` y mostró valores no nulos para
  los cuatro registros; los tres primeros compartieron el timestamp de
  publicación de su batch y el cuarto tuvo el timestamp posterior de su
  publicación.
- Se detuvo el consumer sin borrar `data/`. Antes del reinicio,
  `data/checkpoint/bronze/offsets/1` registraba el fin `{"0":3}` para el
  topic. Después del reinicio, el checkpoint generó `offsets/2` con
  `{"0":4}`.
- Se publicó un cuarto evento y Redpanda asignó `partition=0 offset=3`.
  Bronze lo mostró con ese offset junto a los tres registros anteriores. Una
  consulta por `eventId` devolvió una fila para cada uno de los cuatro IDs;
  ningún ID controlado se reinsertó en este reinicio ordenado.
- El test local `streaming/test_bronze_ingest.py` pasó con
  `NEXA_BRONZE_PROJECTION_TEST_OK`, comprobando la proyección exacta de los
  seis campos y la preservación del JSON.
- `data/` contiene únicamente datos Delta y archivos operativos del checkpoint,
  y quedó excluido por `.gitignore`.

### 15.5 Límites y pendientes

La verificación del camino Kafka-to-Bronze se hizo con eventos controlados
publicados mediante `rpk`. Ratchet y sus microservicios no estaban levantados
en esta sesión, por lo que no se verificó el recorrido end-to-end desde una
operación HTTP de Ratchet ni se ejecutaron scripts k6. Eso no invalida la
integración real con Redpanda, pero deja esa validación adicional pendiente.

La prueba de reinicio demuestra recuperación con datos y checkpoint conservados
durante un stop ordenado, y en esa ejecución no reinsertó los cuatro `eventId`
controlados. No forcé una caída en el momento crítico. La documentación de
Delta respalda la idempotencia del commit del sink nativo para la misma consulta
y checkpoint, pero eso no es una promesa de exactly-once global ni de unicidad
de negocio: dos publicaciones Kafka con offsets distintos siguen siendo dos
filas posibles en Bronze.

La prueba usó una sola partición y no pretende demostrar balanceo entre
particiones o carga sostenida. El Compose de Ratchet todavía no declara la red
compartida: la conexión de Redpanda se hizo dinámicamente y debe repetirse si
el contenedor es recreado. Ratchet quedó sin modificaciones.

Los cambios se dejaron sin commit para una revisión independiente posterior.

## 16. Hito 3 — aprendizaje, decisiones y verificación

### 16.1 Qué entendí antes de implementar

La primera aclaración fue separar la responsabilidad de las capas. `value` es
el JSON del evento que Ratchet publicó; Bronze lo conserva crudo junto con la
traza técnica de Kafka. Bronze es evidencia de entrada y no se modifica para
que Silver pueda corregirlo. Silver lee Bronze mediante otra consulta de Spark
y produce una vista curada para medir, sin borrar ni reescribir la fuente.

También separé tres tiempos distintos: `occurredAt` indica cuándo ocurrió el
hecho de negocio; el timestamp de Kafka indica cuándo el broker registró el
mensaje; y el tiempo de procesamiento indica cuándo Spark lo observó. Silver
usa `occurredAt`, porque las ventanas deben representar el tiempo del evento y
no el momento en que Spark llegó a procesarlo.

Una ventana *tumbling* divide el tiempo en bloques consecutivos de cinco
minutos (`00–05`, `05–10`, etc.). Una ventana *sliding* se solaparía, pero no
es necesaria para este hito. El watermark tampoco es un temporizador pegado al
último registro: Spark lo calcula a partir del máximo tiempo de evento que ha
visto menos cinco minutos. Así puede cerrar estado viejo sin esperar para
siempre y todavía aceptar datos razonablemente atrasados.

Finalmente distinguí el estado de la consulta, su checkpoint y el log Delta.
El estado contiene información intermedia de ventanas y deduplicación; el
checkpoint de Spark guarda el progreso y ese estado para reanudar; el log
transaccional de Delta registra los commits de las tablas. El conteo de eventos
no es lo mismo que el conteo de usuarios distintos: `event_count` cuenta
eventos válidos después de deduplicar `eventId`, mientras que
`distinct_users` estima valores únicos de `holderRef` con HyperLogLog. HLL
reduce memoria, pero no deduplica eventos.

### 16.2 Diseño aprobado

- Crear un job independiente en `streaming/silver_aggregate.py`; no cambiar el
  job de Bronze.
- Leer la tabla Delta Bronze con `readStream`, incluyendo el snapshot inicial y
  los commits nuevos, y usar un checkpoint propio en
  `data/checkpoint/silver`.
- Parsear el JSON con un esquema explícito. Aceptar `eventVersion = 1` y los
  cinco tipos del contrato de Ratchet. Exigir los campos de nivel superior y
  exigir `holdId` salvo para `RESERVATION_REJECTED`, que puede no tenerlo.
- Dejar los inválidos en Bronze y excluirlos de las métricas. En este hito se
  observan mediante una salida de consola, sin crear otra tabla de rechazados.
- Usar ventanas *tumbling* de cinco minutos, watermark de cinco minutos,
  deduplicación por `eventId` limitada por ese estado y
  `approx_count_distinct(holderRef, rsd = 0.05)`.
- Escribir en Silver en modo append las columnas de ventana, tipo de evento,
  cantidad de eventos y usuarios distintos. No deduplicar por `holdId` ni por
  `holderRef`.
- Esperar a que exista `_delta_log` de Bronze antes de iniciar Silver y no
  reutilizar ni borrar checkpoints.

Quedaron explícitamente fuera las anomalías, Gold/DuckDB, reportes, LLM,
FastAPI, Cypher y cualquier modificación de Ratchet.

### 16.3 Implementación y ajustes

La implementación agregó el job, una prueba unitaria de parsing/agregación,
una prueba de integración con tablas Delta temporales y el servicio
`spark-silver`. El servicio necesita solamente Delta: no consume Kafka porque
Bronze ya es su fuente.

La primera prueba reveló que `from_json` puede devolver un struct nulo para
JSON inválido sin distinguirlo por sí solo de campos faltantes. Se agregó una
comprobación mínima con `get_json_object` para conservar el motivo
`invalid_json`. La revisión de validación también detectó que un
`eventVersion` ausente podía pasar por la lógica de tres valores de Spark; se
lo marca explícitamente como `unsupported_event_version`.

### 16.4 Evidencia de verificación

- `docker compose config --quiet` terminó con código `0` después de agregar
  `spark-silver` y sus directorios persistentes.
- `streaming/test_silver_aggregate.py` pasó en Spark 3.5.8 con
  `NEXA_SILVER_PROJECTION_TESTS_OK`.
- `streaming/test_silver_stream.py` pasó con Delta Lake 3.3.0 y
  `NEXA_SILVER_STREAM_TESTS_OK`. Cubrió snapshot inicial, ventanas exactas,
  evento atrasado dentro del watermark, evento posterior al cierre y
  recuperación desde checkpoint sin reinsertar una ventana finalizada.
- La regresión de Hito 2 pasó con `NEXA_BRONZE_PROJECTION_TEST_OK`.
- El smoke test de Hito 1 pasó con
  `NEXA_SMOKE_TEST_OK spark=3.5.8 python=3.10.12 java=17.0.17 master=local[2] count=1`.

### 16.5 Límites actuales

Silver no promete que una métrica permanezca abierta indefinidamente: los
eventos más antiguos que el watermark pueden descartarse del estado de Spark.
La deduplicación está acotada por esa misma retención y no convierte Bronze en
una tabla de negocio única. HLL es una aproximación con error relativo
configurado, no un conteo exacto. Los inválidos siguen auditables en Bronze,
pero todavía no tienen una tabla de rechazados persistida.

La verificación de este cierre usó Delta local con directorios temporales; no
se repitió en esta sesión una publicación end-to-end desde Ratchet hacia un
Redpanda activo. Los cambios de Hito 3 quedaron sin commit hasta una revisión
final y autorización explícita.
