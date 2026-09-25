# 12 · Estructura del proyecto y build

## 1. Árbol del repositorio

```
kafka-testkit/
├── pom.xml                                   # padre: versiones, plugins, módulos
├── README.md
├── LICENSE                                   # Apache-2.0 (propuesta)
├── CHANGELOG.md
├── .github/workflows/
│   ├── build.yml                             # verify en cada push/PR
│   └── release.yml                           # publicación al crear tag vX.Y.Z
├── .mvn/wrapper/ + mvnw, mvnw.cmd            # Maven Wrapper
├── config/
│   ├── checkstyle.xml
│   └── spotbugs-exclude.xml
├── docs/                                     # esta documentación
├── kafka-testkit-bom/pom.xml
├── kafka-testkit-core/
│   ├── pom.xml
│   └── src/
│       ├── main/java/io/github/volumidev/kafkatestkit/...
│       ├── main/java/module-info.java
│       ├── main/resources/META-INF/services/io.github.volumidev.kafkatestkit.serde.SerdeProvider
│       └── test/java/...                     # unitarios
├── kafka-testkit-schema-registry/
├── kafka-testkit-karate/
├── kafka-testkit-testcontainers/
└── kafka-testkit-it/
    └── src/test/
        ├── java/...                          # ITs JUnit
        └── resources/
            ├── kafka-testkit.yml
            ├── schemas/ (avsc, proto, json)
            ├── certs/   (generados para tests de seguridad)
            └── features/ (karate)
```

## 2. Paquetes de `kafka-testkit-core`

```
io.github.volumidev.kafkatestkit
├── KafkaTestKit                 interfaz principal + factorías load()
├── KafkaTestKitBuilder
├── ClusterClient
├── TopicClient
├── TopicOptions
├── config/
│   ├── KafkaTestKitConfig       record raíz
│   ├── ClusterConfig, TopicConfig, SerdeConfig, SchemaRegistryConfig
│   ├── SecurityConfig, SslConfig, SaslConfig (sealed) + variantes
│   ├── AdminPolicy, Defaults, Timeouts, TopicDefinition
│   ├── Secret
│   └── ConfigError
├── message/
│   ├── KafkaMessage, Headers, OutgoingMessage, SendResult
│   ├── ReadSpec, DeserializationProblem
├── capture/
│   ├── MessageCapture, MultiTopicCapture, CaptureOptions
│   ├── MessageFilter, Filters, TopicFilter
├── admin/
│   ├── MetadataClient, TopicAdmin, ConsumerGroupClient
│   ├── ClusterInfo, BrokerInfo, TopicDescription, PartitionInfo, TopicOffsets, PartitionOffsets
│   ├── GroupListing, GroupDescription, MemberInfo, GroupLag, PartitionLag
│   ├── NewTopicSpec, OffsetResetSpec, TopicDefinitionReport
├── serde/
│   ├── SerdeFormat, SerdeProvider, SerdeContext, TestSerde
├── schema/
│   ├── SchemaRegistryClient, SchemaInfo, SchemaType, CompatibilityResult   (interfaces; impl en módulo SR)
├── exception/
│   └── (ver 11)
└── internal/                    NO API pública
    ├── config/   YamlConfigLoader, Interpolator, EnvironmentMerger, ConfigValidator,
    │             SecurityPropertiesMapper, ResourceLoader, DurationParser, PropertiesMasker
    ├── client/   ClientPool, ProducerFactory, ConsumerFactory, AdminFactory
    ├── produce/  DefaultTopicClient, MessageFileParser
    ├── read/     Reader, OffsetResolver
    ├── capture/  DefaultMessageCapture, CaptureWorker, CaptureBuffer, TimeoutDiagnostics
    ├── admin/    DefaultMetadataClient, DefaultTopicAdmin, DefaultConsumerGroupClient, DestructivePolicy
    ├── serde/    SerdeRegistry, BuiltinSerdeProvider, JsonSerde, StringSerde, BytesSerde, NumberSerde
    └── util/     MapPaths, NumberNormalizer, Json
```

Módulo SR: `io.github.volumidev.kafkatestkit.schemaregistry` (`ConfluentSerdeProvider`, `AvroSerde`, `AvroMapConverter`, `ProtobufSerde`, `ProtobufMapConverter`, `JsonSchemaSerde`, `DefaultSchemaRegistryClient`).

Módulo Karate: `io.github.volumidev.kafkatestkit.karate` (`KarateKafka`, `KarateCapture`, `KarateMultiCapture`, `KarateFilters`, `KarateDurations`).

Módulo Testcontainers: `io.github.volumidev.kafkatestkit.testcontainers` (`EmbeddedKafka`, `EmbeddedKafkaOptions`, `EmbeddedSecurity`, `KafkaTestKitContainers`, `InjectKafkaTestKit`, `KafkaTestKitExtension`).

## 3. `pom.xml` padre (extracto)

```xml
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>io.github.volumidev</groupId>
  <artifactId>kafka-testkit-parent</artifactId>
  <version>0.1.0-SNAPSHOT</version>
  <packaging>pom</packaging>

  <modules>
    <module>kafka-testkit-bom</module>
    <module>kafka-testkit-core</module>
    <module>kafka-testkit-schema-registry</module>
    <module>kafka-testkit-karate</module>
    <module>kafka-testkit-testcontainers</module>
    <module>kafka-testkit-it</module>
  </modules>

  <properties>
    <maven.compiler.release>21</maven.compiler.release>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>

    <kafka.version>4.1.0</kafka.version>
    <confluent.version>8.0.0</confluent.version>
    <jackson.version>2.19.0</jackson.version>
    <jsonpath.version>2.9.0</jsonpath.version>
    <slf4j.version>2.0.17</slf4j.version>
    <karate.version>1.5.1</karate.version>
    <testcontainers.version>1.21.0</testcontainers.version>
    <junit.version>5.12.2</junit.version>
    <assertj.version>3.27.3</assertj.version>
  </properties>

  <repositories>
    <repository>
      <id>confluent</id>
      <url>https://packages.confluent.io/maven/</url>
    </repository>
  </repositories>

  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>com.fasterxml.jackson</groupId>
        <artifactId>jackson-bom</artifactId>
        <version>${jackson.version}</version>
        <type>pom</type><scope>import</scope>
      </dependency>
      <dependency>
        <groupId>org.testcontainers</groupId>
        <artifactId>testcontainers-bom</artifactId>
        <version>${testcontainers.version}</version>
        <type>pom</type><scope>import</scope>
      </dependency>
      <dependency>
        <groupId>org.junit</groupId>
        <artifactId>junit-bom</artifactId>
        <version>${junit.version}</version>
        <type>pom</type><scope>import</scope>
      </dependency>
      <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-clients</artifactId>
        <version>${kafka.version}</version>
      </dependency>
      <!-- confluent, json-path, slf4j, karate, assertj ... -->
    </dependencies>
  </dependencyManagement>
</project>
```

> Las versiones son orientativas; se fijarán a las últimas estables al crear el esqueleto (fase 1 del roadmap), comprobando que `kafka-clients` y Confluent son compatibles entre sí.

## 4. Plugins de build y calidad

| Plugin | Fase | Propósito |
|--------|------|-----------|
| `maven-enforcer-plugin` | validate | Java ≥ 21, Maven ≥ 3.9, sin dependencias duplicadas/convergencia |
| `spotless-maven-plugin` | validate (`check`) | Formato `google-java-format` (AOSP) + licencia en cabecera |
| `maven-compiler-plugin` | compile | `release 21`, `-Xlint:all -Werror` |
| `maven-surefire-plugin` | test | Tests unitarios (`*Test`) |
| `maven-failsafe-plugin` | integration-test/verify | Tests de integración (`*IT`), solo en `kafka-testkit-it` |
| `jacoco-maven-plugin` | verify | Cobertura; umbral 80 % líneas en core |
| `maven-checkstyle-plugin` | verify | Reglas de estilo/diseño |
| `spotbugs-maven-plugin` | verify | Análisis estático |
| `maven-javadoc-plugin` | package | Javadoc de API pública (`-Xdoclint:all` excepto `missing` en internal) |
| `maven-source-plugin` | package | Jar de fuentes |
| `flatten-maven-plugin` | process-resources | POMs publicados limpios (`${revision}` CI-friendly) |
| `maven-gpg-plugin` + `central-publishing-maven-plugin` | deploy (perfil `release`) | Publicación en Maven Central |
| `license-maven-plugin` | verify | Informe de licencias de dependencias |

Perfiles:

| Perfil | Uso |
|--------|-----|
| (defecto) | Compila y pasa tests unitarios; ITs se omiten si no hay Docker (`@Testcontainers(disabledWithoutDocker = true)`) |
| `it` | Fuerza tests de integración |
| `release` | Firma y publica |
| `corporate` | `distributionManagement` a Nexus/Artifactory corporativo (URL por propiedad) |

## 5. CI (GitHub Actions)

```yaml
name: build
on: [push, pull_request]
jobs:
  verify:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        java: [ '21', '25' ]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with: { distribution: temurin, java-version: '${{ matrix.java }}', cache: maven }
      - run: ./mvnw -B -ntp verify -Pit
      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: reports-${{ matrix.java }}
          path: |
            **/target/surefire-reports
            **/target/failsafe-reports
            **/target/karate-reports
            **/target/site/jacoco
```

`release.yml`: al empujar un tag `vX.Y.Z` → `./mvnw -Prelease -Drevision=X.Y.Z deploy` + GitHub Release con el `CHANGELOG`.

## 6. Versionado y compatibilidad

- **SemVer**: `MAJOR.MINOR.PATCH`. Hasta `1.0.0` la API puede cambiar entre minors.
- API pública = paquetes no `internal`. Se valida con `japicmp-maven-plugin` desde 1.0.0 (falla el build si se rompe compatibilidad en un minor).
- El formato YAML es también API pública: claves nuevas → minor; eliminar/renombrar → major (con periodo de deprecación en el que se acepta la clave vieja con WARN).
- `CHANGELOG.md` con formato *Keep a Changelog*.

## 7. Convenciones de código

- Records para modelos inmutables; `sealed` para jerarquías cerradas (SASL, posiciones de lectura).
- `Optional` solo en retornos, nunca en parámetros ni campos de records públicos salvo modelos de metadatos.
- Sin `null` en la API pública salvo `KafkaMessage.key/value` (Kafka los permite) — documentado.
- Nada de estado estático mutable (excepto cachés explícitas de `KarateKafka` y `EmbeddedKafka.shared()`).
- Duraciones siempre como `java.time.Duration` en Java, strings en YAML/Karate.
- Nombres de tests: `metodo_condicion_resultado` o frases en inglés con `@DisplayName`.
