# Documentación de diseño — kafka-testkit

Este directorio recoge el diseño completo de la librería antes de empezar la implementación. Cada documento es autocontenido, pero se recomienda leerlos en orden la primera vez.

## Mapa de documentos

| # | Documento | Para quién | Qué responde |
|---|-----------|------------|--------------|
| 01 | [Visión y alcance](01-vision-y-alcance.md) | Todos | ¿Qué problema resuelve? ¿Qué queda fuera? Requisitos y glosario |
| 02 | [Arquitectura](02-arquitectura.md) | Desarrolladores de la librería | Módulos, componentes, ciclo de vida, concurrencia, decisiones |
| 03 | [Configuración YAML](03-configuracion-yaml.md) | Usuarios | Referencia completa de todas las claves del YAML |
| 04 | [Seguridad](04-seguridad.md) | Usuarios / DevOps | Cómo configurar PLAINTEXT, SSL, mTLS, SASL, OAuth, Kerberos |
| 05 | [API core](05-api-core.md) | Usuarios Java | Cómo producir y leer mensajes; modelo de mensaje |
| 06 | [Capturas y esperas](06-capturas-y-esperas.md) | Usuarios | Cómo esperar mensajes sin condiciones de carrera; filtros |
| 07 | [Metadatos, admin y grupos](07-metadatos-admin-grupos.md) | Usuarios | Topics, particiones, offsets, lag, crear/purgar topics |
| 08 | [Serdes y Schema Registry](08-serdes-y-schema-registry.md) | Usuarios / Desarrolladores | Formatos soportados, Avro/Protobuf/JSON Schema, SPI |
| 09 | [Integración con Karate](09-integracion-karate.md) | Usuarios Karate | Fachada `KarateKafka`, ejemplos de features |
| 10 | [Testcontainers](10-testcontainers.md) | Usuarios | Kafka embebido para tests aislados |
| 11 | [Errores, logging y observabilidad](11-errores-logging-observabilidad.md) | Todos | Excepciones, mensajes de diagnóstico, logs |
| 12 | [Estructura del proyecto y build](12-estructura-proyecto-y-build.md) | Desarrolladores | Árbol de paquetes, poms, calidad, CI, publicación |
| 13 | [Estrategia de testing](13-estrategia-de-testing.md) | Desarrolladores | Cómo se prueba la propia librería |
| 14 | [Roadmap](14-roadmap.md) | Todos | Fases de implementación y criterios de aceptación |

## Convenciones de esta documentación

- **Nombres provisionales**: groupId `io.github.volumidev`, artefactos `kafka-testkit-*`, paquete raíz `io.github.volumidev.kafkatestkit`. Se pueden cambiar antes de la primera release.
- Los fragmentos de código son **ilustrativos del diseño**: fijan firmas y comportamiento esperado, no son la implementación final.
- "Nombre lógico" = el nombre que se usa en el YAML y en los tests (`orders`). "Nombre físico" = el nombre real del topic en Kafka (`dev.orders.v1`).
- Las duraciones se escriben como en el YAML: `500ms`, `20s`, `2m`.

## Decisiones clave (resumen)

| Decisión | Valor |
|----------|-------|
| Lenguaje / build | Java 21, Maven multi-módulo |
| Cliente Kafka | `org.apache.kafka:kafka-clients` 4.x |
| Formatos | STRING, JSON, BYTES, LONG, INTEGER, AVRO, PROTOBUF, JSON_SCHEMA |
| Schema Registry | Confluent Schema Registry (serializers Confluent 8.x) |
| Seguridad | PLAINTEXT, SSL (incl. mTLS), SASL PLAIN, SCRAM-SHA-256/512, OAUTHBEARER, GSSAPI |
| Lectura | `assign()` + `seek()` sin consumer group por defecto |
| Esperas | `MessageCapture` en segundo plano iniciada antes de la acción |
| Integración | Núcleo Java puro + adaptador Karate |
| Tests de la librería | JUnit 6 + Testcontainers 2 + suite Karate 2 |
