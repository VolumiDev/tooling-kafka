# Changelog

Todos los cambios relevantes de este proyecto se documentan en este fichero.

El formato sigue [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/) y el proyecto usa [Semantic Versioning](https://semver.org/lang/es/).

## [Unreleased]

### Añadido
- Esqueleto Maven multi-módulo: `core`, `schema-registry`, `karate`, `testcontainers`, `bom` e `it` (F1).
- Maven Wrapper, enforcer, Spotless (google-java-format AOSP + cabecera Apache-2.0), Checkstyle, SpotBugs, JaCoCo (≥ 80 % en core), surefire/failsafe y perfil `it`.
- `KafkaTestKitVersion` con la versión de la librería.
- Workflow de CI `build.yml` (Java 21 y 25).
