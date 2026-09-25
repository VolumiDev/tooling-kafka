# kafka-testkit

Librería Java 21 (Maven multi-módulo) para **testing** sobre Kafka: producir y consumir, capturas con esperas, metadatos, admin, consumer groups y Schema Registry. Se usa desde Karate y desde cualquier herramienta Java. Toda la configuración está en YAML. NO es una librería para crear productores ni consumidores de producción.

## Fuente de verdad
- El diseño está en `docs/` (índice: `docs/00-index.md`). Léelo antes de implementar algo.
- Estado y siguiente fase: `docs/14-roadmap.md`. Trabaja fase a fase.
- Si el código se aparta del diseño, actualiza el `.md` correspondiente en el mismo cambio.

## Comandos
- Build + tests unitarios: `./mvnw verify` (en Windows `mvnw.cmd verify`)
- Con integración (requiere Docker): `./mvnw verify -Pit`
- Un módulo: `./mvnw -pl kafka-testkit-core -am verify`
- Formato: `./mvnw spotless:apply`

## Módulos
core · schema-registry · karate · testcontainers · bom · it (no se publica). Core NO depende de Confluent, Karate ni Testcontainers.

## Reglas de código
- Paquete raíz `io.github.volumidev.kafkatestkit` (provisional). La API pública va fuera de `internal`; la implementación, en `internal.*`.
- Records para modelos inmutables, `sealed` para jerarquías cerradas, `Duration` en Java y strings (`20s`) en YAML y Karate.
- Nada de estado estático mutable (excepción: cachés de `KarateKafka` y `EmbeddedKafka.shared()`).
- Solo `slf4j-api` para logs. Nunca registrar secretos (usar `Secret` y `PropertiesMasker`).
- Excepciones unchecked de `exception/`. Las esperas fallidas extienden `AssertionError`.
- Lectura por defecto con `assign()` + `seek()`, sin `group.id`.
- Los filtros se evalúan en el hilo que llama al await (requisito de GraalJS en Karate).
- Javadoc en toda la API pública. Formato google-java-format (Spotless).

## Tests
- Unitarios `*Test` (surefire, sin Docker). Integración `*IT` en `kafka-testkit-it` (failsafe + Testcontainers).
- Prohibido `Thread.sleep` en tests salvo en `assertNone`. Inyecta `Clock` para los timeouts.
- Cobertura core ≥ 80 %.

## Flujo de trabajo
- Una fase del roadmap por rama (`feat/fN-nombre`) y PR. Commits pequeños; no hacer push sin pedirlo.
- Documentación y comentarios en español; identificadores de código en inglés.
