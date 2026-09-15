# AGENTS.md — Nexa

## Propósito del proyecto

Nexa es un pipeline self-hosted de Data Engineering que procesa eventos reales
de reservas publicados por Ratchet.

Su arquitectura prevista es:

Ratchet
→ Redpanda/Kafka
→ Spark Structured Streaming
→ Delta Lake Bronze
→ Delta Lake Silver
→ detección determinística de anomalías
→ DuckDB Gold
→ generación periódica de reportes
→ API protegida mediante tokens de Cypher

El núcleo del proyecto es la ingeniería de datos: ingesta, streaming,
transformación, arquitectura medallion, tolerancia a fallos, trazabilidad y
verificación. La capa LLM es complementaria: redacta explicaciones, pero nunca
decide qué constituye una anomalía.

Todo debe funcionar localmente y con costo permanente cero.

## Fuentes de verdad

Antes de proponer o implementar cambios sustanciales, inspeccioná:

1. El código y los tests actuales.
2. `git status`, el diff y el historial reciente.
3. `docs/bitacora-nexa.md`.
4. `README.md`.
5. `Proyecto_Nexa.md`, cuando exista localmente.

Aplicá esta precedencia:

- Código, tests y Git: estado realmente implementado.
- Bitácora: decisiones, aprendizaje y cierres verificados.
- README: instalación, operación y uso público actual.
- Proyecto_Nexa.md: visión y planificación privada, que puede contener estados
  históricos desactualizados.

No repitas como vigente una afirmación contradicha por evidencia más reciente.

`Proyecto_Nexa.md` está ignorado localmente y es privado. No lo publiques,
agregues a Git ni modifiques salvo que la tarea sea explícitamente revisar o
actualizar la planificación.

## Estado base conocido

Como referencia histórica, no como sustituto de la inspección:

- Hito 1 estableció Spark 3.5.8, Python 3.10, Java 17 y ejecución `local[2]`.
- Hito 2 implementó Redpanda/Kafka → Spark Structured Streaming → Delta Bronze.
- El commit de cierre del Hito 2 es
  `5aaf17e feat: implement Bronze streaming ingestion`.
- Bronze conserva el JSON original y los metadatos Kafka necesarios.
- Bronze y sus checkpoints viven bajo `data/`, que no debe versionarse.
- El próximo trabajo originalmente previsto era Silver, ventanas, watermark y
  conteo aproximado, pero el roadmap debe verificarse antes de asumirlo vigente.

## Integraciones y contratos

Ratchet es la única fuente de datos de Nexa:

- Tópico: `reservation.events.v1`.
- Kafka message key: `resourceId`.
- Contrato JSON versionado mediante `eventVersion`.
- Tipos conocidos:
  - `RESERVATION_HOLD_CREATED`
  - `RESERVATION_CONFIRMED`
  - `RESERVATION_RELEASED`
  - `RESERVATION_EXPIRED`
  - `RESERVATION_REJECTED`
- `eventId` identifica el evento.
- `holdId` identifica una reserva retenida y no existe necesariamente en todos
  los tipos, especialmente en `RESERVATION_REJECTED`.
- Ratchet publica con entrega at-least-once; los consumidores de negocio deben
  considerar duplicados por `eventId`.

Nexa es consumidor puro. No llama a Ratchet por REST, no escribe en sus bases
y no modifica Ratchet salvo autorización explícita para una tarea separada.

Cypher no participa en el pipeline interno. Su función futura se limita a
proteger la API externa mediante validación local de JWT RS256 contra su JWKS.

## Restricciones arquitectónicas

- Infraestructura con costo permanente cero.
- Todo self-hosted y reproducible con Docker Compose.
- Python/PySpark como lenguaje principal.
- Spark Structured Streaming y Delta Lake para Bronze/Silver.
- DuckDB como candidato para Gold, sujeto a revisión antes de implementarlo.
- Reglas determinísticas y auditables para detectar anomalías.
- El LLM puede explicar resultados ya calculados; nunca clasificarlos.
- No implementar hitos futuros anticipadamente.
- No agregar servicios, dependencias, abstracciones o configuraciones
  especulativas.
- No usar Databricks, Snowflake, Confluent Cloud, Redpanda Cloud ni otro SaaS
  pago como dependencia de v1.
- Verificar compatibilidad y comportamiento con documentación oficial de las
  versiones realmente utilizadas.

## Metodología de aprendizaje

Para un nuevo hito o concepto sustancial, usar:

explicación
→ explicación del usuario con sus propias palabras
→ corrección de dudas
→ diseño breve
→ una aprobación explícita
→ implementación
→ verificación
→ revisión del diff
→ documentación
→ commit autorizado

No convertir detalles triviales en clases teóricas.

Antes de implementar un hito, presentar en el chat:

- qué se construirá;
- por qué es necesario;
- qué archivos cambiarán;
- cómo se verificará;
- qué queda explícitamente afuera.

Esperar una sola aprobación explícita del diseño. No crear documentos
intermedios solo para solicitar otra aprobación equivalente.

Si la explicación del usuario contiene un error, corregirlo con evidencia. No
aceptarlo por deferencia.

## Implementación y pruebas

- Buscar primero patrones y helpers existentes.
- Preferir funciones nativas de Spark, Python y las dependencias ya instaladas.
- Para lógica no trivial, escribir primero la comprobación mínima que pueda
  fallar.
- Para infraestructura, dejar una validación ejecutable equivalente.
- Probar casos negativos, recuperación y límites relevantes, no solo el caso
  feliz.
- No borrar checkpoints o datos reales para hacer pasar una prueba.
- Usar rutas temporales para pruebas destructivas o de recuperación.
- No afirmar que algo funciona sin una verificación reciente.
- Distinguir propiedades respaldadas por documentación de comportamientos
  demostrados experimentalmente.
- Watermarking, checkpointing y detección de anomalías requieren una revisión
  independiente antes del commit.

## Documentación

Mantener únicamente:

- `README.md`: documentación pública y operativa, siempre en inglés.
- `docs/bitacora-nexa.md`: decisiones, aprendizaje y cierres, en español.
- `Proyecto_Nexa.md`: planificación privada, ignorada por Git.

No crear specs, ADRs, reportes o planes paralelos salvo necesidad concreta y
aprobación explícita.

No inventar texto presentado como si fuera la explicación personal del usuario.
La bitácora debe reflejar lo que realmente entendió y expresó.

## Git y seguridad

- Preservar cambios ajenos.
- No usar operaciones destructivas para limpiar el workspace.
- Revisar `git status`, `git diff`, `git diff --cached` y el diff completo antes
  de commitear.
- Commits pequeños, completos y con mensajes en inglés.
- No hacer commit ni push sin autorización explícita.
- Nunca versionar `data/`, checkpoints, archivos Delta generados, caches,
  secretos, `.env` ni documentación privada.
- No iniciar simultáneamente escritores Desktop y CLI sobre la misma sesión.

## Comunicación

Responder en español, salvo contenido que deba quedar en inglés dentro del
repositorio.

Diferenciar claramente:

- hechos verificados;
- supuestos;
- recomendaciones;
- decisiones pendientes.

Ser directo, didáctico y crítico. Priorizar la solución mínima correcta y
reproducible. Ante una ambigüedad que cambie materialmente el resultado,
consultar al usuario en vez de inventar la decisión.
