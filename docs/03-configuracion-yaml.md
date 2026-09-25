# 03 · Configuración YAML (referencia)

Toda la configuración de la librería vive en un fichero YAML. Este documento es la referencia completa de claves, valores por defecto y reglas de resolución.

## 1. Localización del fichero

Orden de resolución (gana el primero que exista):

1. Argumento explícito: `KafkaTestKit.load("ruta/o/classpath")`.
2. Propiedad de sistema `-Dkafka.testkit.config=...`.
3. Variable de entorno `KAFKA_TESTKIT_CONFIG`.
4. `kafka-testkit.yml` en el classpath.
5. `kafka-testkit.yaml` en el classpath.

Formatos de ruta admitidos:

| Prefijo | Ejemplo | Significado |
|---------|---------|-------------|
| `classpath:` | `classpath:config/kafka-dev.yml` | Recurso del classpath |
| `file:` o sin prefijo | `./conf/kafka.yml`, `/etc/kafka-testkit.yml` | Sistema de ficheros |

Se pueden **combinar varios ficheros** (el último sobrescribe a los anteriores), útil para separar topics de clusters:

```java
KafkaTestKit.load(List.of("classpath:kafka-clusters.yml", "classpath:kafka-topics.yml"), "dev");
```

o mediante `include` en el propio YAML:

```yaml
include:
  - classpath:kafka-clusters.yml
  - classpath:topics/orders.yml
```

## 2. Estructura general

```yaml
include: [...]            # opcional: otros ficheros a fusionar antes que este
defaults: {...}           # valores por defecto globales
clusters: {...}           # mapa nombreLógico -> cluster
topics: {...}             # mapa nombreLógico -> topic
environments: {...}       # mapa entorno -> overrides parciales de todo lo anterior
testcontainers: {...}     # opcional: solo lo lee el módulo testcontainers (ver 10-testcontainers.md)
```

## 3. `defaults`

```yaml
defaults:
  timeouts:
    await: 30s            # espera por defecto en awaitOne/awaitCount/awaitLag
    poll: 200ms           # intervalo de poll de capturas y lecturas
    request: 15s          # timeout de send(), admin y metadatos
    read: 10s             # tiempo máximo de read() si no se indica
  capture:
    maxBufferedMessages: 10000   # tope del buffer de una captura (FIFO: descarta los más antiguos)
    diagnosticsSample: 10        # nº de mensajes resumidos en errores de timeout
  producer:
    properties:           # propiedades nativas para TODOS los producers
      acks: all
      linger.ms: 0
      enable.idempotence: true
  consumer:
    properties:           # propiedades nativas para TODOS los consumers
      max.poll.records: 500
      isolation.level: read_committed
  admin:
    properties: {}
  serde:
    key:   { format: STRING }  # formato por defecto si el topic no lo indica
    value: { format: JSON }
  logging:
    payloads: TRUNCATED   # NONE | TRUNCATED | FULL
    maxPayloadChars: 500
```

| Clave | Tipo | Defecto |
|-------|------|---------|
| `timeouts.await` | duración | `30s` |
| `timeouts.poll` | duración | `200ms` |
| `timeouts.request` | duración | `15s` |
| `timeouts.read` | duración | `10s` |
| `capture.maxBufferedMessages` | int | `10000` |
| `capture.diagnosticsSample` | int | `10` |
| `serde.key.format` | `SerdeFormat` | `STRING` |
| `serde.value.format` | `SerdeFormat` | `JSON` |
| `logging.payloads` | enum | `TRUNCATED` |

Duraciones: `<número><unidad>` con unidades `ms`, `s`, `m`, `h`; también ISO-8601 (`PT30S`).

## 4. `clusters`

```yaml
clusters:
  main:                                   # nombre lógico
    bootstrapServers: broker1:9093,broker2:9093
    clientIdPrefix: kafka-testkit-qa      # defecto: kafka-testkit
    security: {...}                       # ver 04-seguridad.md
    schemaRegistry: {...}                 # §4.1
    admin:                                # política de operaciones destructivas, §4.2
      allowDestructive: false
      protectedTopics: [ "^_.*", "^prod\\..*" ]
    properties: {...}                     # nativas, se aplican a producer/consumer/admin de este cluster
    producer: { properties: {...} }       # nativas solo producer
    consumer: { properties: {...} }       # nativas solo consumer
    admin-client: { properties: {...} }   # nativas solo admin
```

| Clave | Obligatoria | Descripción |
|-------|-------------|-------------|
| `bootstrapServers` | Sí | Lista separada por comas o lista YAML |
| `clientIdPrefix` | No | Prefijo de `client.id`; se añade `-producer-N`, `-capture-<topic>-N`… (útil para localizar al cliente en logs de broker) |
| `security` | No | Por defecto `protocol: PLAINTEXT` |
| `schemaRegistry` | Si algún topic del cluster usa AVRO/PROTOBUF/JSON_SCHEMA | |
| `admin` | No | Política de seguridad de admin |
| `properties` | No | Propiedades nativas de Kafka comunes |

### 4.1 `schemaRegistry`

```yaml
schemaRegistry:
  url: https://sr1.acme.com,https://sr2.acme.com
  auth:
    type: BASIC                 # NONE | BASIC | BEARER | USER_INFO_FROM_SASL
    username: ${SR_USER}
    password: ${SR_PASSWORD}
    # token: ${SR_TOKEN}         # para BEARER
  ssl:                          # si el registry usa TLS con CA propia
    truststore: { location: classpath:certs/sr-truststore.jks, password: ${SR_TS_PASS} }
    keystore:   { location: ..., password: ..., keyPassword: ... }   # mTLS con el registry
  cacheCapacity: 1000
  properties:                   # nativas del cliente/serializers Confluent
    auto.register.schemas: false
    use.latest.version: true
```

| Clave | Defecto | Descripción |
|-------|---------|-------------|
| `url` | — | Una o varias URLs separadas por coma |
| `auth.type` | `NONE` | `USER_INFO_FROM_SASL` reutiliza usuario/contraseña SASL del cluster |
| `cacheCapacity` | `1000` | Tamaño de la caché de esquemas |
| `properties.auto.register.schemas` | `false` | En tests no se registran esquemas salvo que se pida explícitamente |

### 4.2 `admin` (política de protección)

```yaml
admin:
  allowDestructive: false        # defecto false: prohíbe delete/purge/reset/deleteGroup
  protectedTopics:               # regex; nunca se tocan aunque allowDestructive=true
    - "^__.*"                    # topics internos (__consumer_offsets…)
    - "^_schemas$"
  temporaryTopicPrefix: kt-tmp-  # prefijo de createTemporaryTopic
  allowedTemporaryOnly: false    # si true, solo se puede borrar/purgar lo creado por el kit
```

Siempre están protegidos, aunque no se indique: `^__.*` y `^_schemas$`.

## 5. `topics`

```yaml
topics:
  orders:                                   # nombre lógico
    cluster: main                           # referencia a clusters.<nombre>
    name: ${ENV_PREFIX}.orders.v1           # nombre físico; defecto = nombre lógico
    key:
      format: STRING
    value:
      format: AVRO
      subject: ${ENV_PREFIX}.orders.v1-value   # defecto según subjectNameStrategy
      schemaFile: classpath:schemas/order.avsc # opcional, esquema local para producir
      version: latest                          # latest | <número> | la del schemaFile
    headers:
      defaults:                             # headers añadidos a todos los envíos
        source: kafka-testkit
    definition:                             # usado por Testcontainers / admin.ensureTopic
      partitions: 6
      replicationFactor: 3
      configs:
        retention.ms: 604800000
        cleanup.policy: delete
```

### 5.1 `key` / `value` (`SerdeConfig`)

| Clave | Aplica a | Descripción |
|-------|----------|-------------|
| `format` | todos | `STRING`, `JSON`, `BYTES`, `LONG`, `INTEGER`, `AVRO`, `PROTOBUF`, `JSON_SCHEMA` |
| `charset` | STRING, JSON | Defecto `UTF-8` |
| `subject` | AVRO, PROTOBUF, JSON_SCHEMA | Subject del registry. Defecto: `<nombreFísico>-key` / `-value` (TopicNameStrategy) |
| `subjectNameStrategy` | idem | `TOPIC` (defecto), `RECORD`, `TOPIC_RECORD` |
| `schemaFile` | idem | `.avsc`, `.proto`, `.json` local para producir |
| `version` | idem | `latest` (defecto) o número |
| `messageType` | PROTOBUF | Nombre completo del mensaje si el `.proto` tiene varios |
| `specificClass` | AVRO, PROTOBUF | Clase generada para obtener objetos tipados en vez de `GenericRecord`/`DynamicMessage` |
| `properties` | todos | Propiedades nativas del serializer/deserializer |

Detalle de cada formato en [08 · Serdes](08-serdes-y-schema-registry.md).

### 5.2 Topics dinámicos

No es obligatorio declarar todos los topics. Se puede acceder a uno no declarado indicando el cluster y el formato:

```java
kit.cluster("main").topic("some.physical.topic", TopicOptions.json());
```

Útil para topics temporales o creados en el propio test.

## 6. `environments`

Bloques parciales que se **fusionan en profundidad** sobre la configuración base:

```yaml
clusters:
  main:
    bootstrapServers: kafka-dev.acme.com:9093
    security:
      protocol: SASL_SSL
      sasl: { mechanism: SCRAM-SHA-512, username: ${KAFKA_USER}, password: ${KAFKA_PASSWORD} }
      ssl:  { truststore: { location: classpath:certs/dev-ca.jks, password: ${TS_PASS} } }
    schemaRegistry: { url: https://sr-dev.acme.com }

environments:
  local:
    clusters:
      main:
        bootstrapServers: localhost:9092
        security: { protocol: PLAINTEXT }       # reemplaza el bloque security entero (ver reglas)
        schemaRegistry: { url: http://localhost:8081 }
        admin: { allowDestructive: true }
  pre:
    clusters:
      main:
        bootstrapServers: kafka-pre.acme.com:9093
        schemaRegistry: { url: https://sr-pre.acme.com }
```

Selección del entorno (gana el primero):

1. Argumento `KafkaTestKit.load(path, "pre")`.
2. `-Dkafka.testkit.env=pre`.
3. `KAFKA_TESTKIT_ENV`.
4. `-Dkarate.env` (para integrarse de forma natural con Karate).
5. Ninguno → solo configuración base.

Reglas de merge:

- Mapas: merge recursivo clave a clave.
- Listas y escalares: el entorno reemplaza.
- Bloque `security`: si el entorno cambia `protocol`, el bloque `security` se **reemplaza completo** (evita mezclas incoherentes como PLAINTEXT + SASL). Para mantener el merge recursivo sin cambiar el protocolo, no incluir `protocol`.
- `null` explícito (`schemaRegistry: null`) elimina la clave.
- Un entorno inexistente es un error (`ConfigurationException`), no se ignora.

## 7. Interpolación

Se aplica sobre **valores** (no claves), antes del merge de entornos.

| Sintaxis | Resultado |
|----------|-----------|
| `${VAR}` | Propiedad de sistema `VAR`, si no, variable de entorno `VAR`; si no existe → error |
| `${VAR:defecto}` | Igual, con valor por defecto |
| `${VAR:}` | Por defecto cadena vacía |
| `${env.VAR}` | Solo variable de entorno |
| `${sys.prop.name}` | Solo propiedad de sistema |
| `${file:/ruta/secreto}` | Contenido del fichero (sin salto final); útil con secretos montados en Kubernetes |
| `$${literal}` | Escape: produce `${literal}` |

Se permite anidar en el valor por defecto: `${KAFKA_BOOTSTRAP:${DEFAULT_BOOTSTRAP:localhost:9092}}`.

Los valores resueltos desde claves que contienen `password`, `secret`, `token`, `keyPassword` o `sasl.jaas.config` se marcan como **secretos** y se enmascaran (`****`) en logs y `toString()`.

## 8. Precedencia de propiedades nativas de Kafka

De menor a mayor prioridad, para un producer del cluster `main`:

1. Valores por defecto internos de la librería (p. ej. `enable.auto.commit=false` en consumidores).
2. `defaults.producer.properties`.
3. Propiedades generadas por `security` y `bootstrapServers`.
4. `clusters.main.properties`.
5. `clusters.main.producer.properties`.
6. Propiedades de la operación concreta (p. ej. `ReadSpec.property(...)`).

Excepciones que **no** se pueden sobrescribir (la librería las controla): `key.serializer`, `value.serializer`, `key.deserializer`, `value.deserializer`. Si aparecen en `properties` se emite un warning y se ignoran; los formatos se cambian con `format`.

## 9. Validación

Al cargar se comprueba todo y se acumulan **todos** los errores antes de lanzar la excepción:

| Regla | Ejemplo de mensaje |
|-------|--------------------|
| Topic referencia un cluster inexistente | `topics.orders.cluster: cluster 'mian' no existe. Clusters definidos: [main, legacy]` |
| Formato con esquema sin schema registry | `topics.orders.value.format=AVRO requiere clusters.main.schemaRegistry.url` |
| Módulo de serdes ausente | `Formato AVRO no disponible: añade la dependencia io.github.volumidev:kafka-testkit-schema-registry` |
| Fichero inexistente | `clusters.main.security.ssl.truststore.location: no se encuentra 'classpath:certs/ca.jks'` |
| SASL sin credenciales | `clusters.main.security.sasl.password es obligatorio para SCRAM-SHA-512` |
| Variable sin resolver | `clusters.main.bootstrapServers: la variable KAFKA_BOOTSTRAP no está definida y no tiene valor por defecto` |
| Clave desconocida | `topics.orders.vaule: clave desconocida. ¿Quisiste decir 'value'?` |
| Duración inválida | `defaults.timeouts.await: '30 segundos' no es una duración válida (ej: 500ms, 30s, 2m)` |

Excepción: `ConfigurationException` con `List<ConfigError> errors()`.

## 10. Ejemplo completo

```yaml
include:
  - classpath:kafka-topics-common.yml

defaults:
  timeouts: { await: 30s, poll: 200ms, request: 15s }
  producer:
    properties: { acks: all, enable.idempotence: true }
  consumer:
    properties: { isolation.level: read_committed }

clusters:
  main:
    bootstrapServers: ${KAFKA_MAIN_BOOTSTRAP}
    clientIdPrefix: qa-${USER:ci}
    security:
      protocol: SASL_SSL
      sasl:
        mechanism: SCRAM-SHA-512
        username: ${KAFKA_USER}
        password: ${KAFKA_PASSWORD}
      ssl:
        truststore: { location: classpath:certs/truststore.jks, password: ${TS_PASS}, type: JKS }
    schemaRegistry:
      url: ${SR_URL}
      auth: { type: BASIC, username: ${SR_USER}, password: ${SR_PASSWORD} }
    admin:
      allowDestructive: false
      protectedTopics: [ "^prod\\..*" ]

  legacy:
    bootstrapServers: legacy-kafka.acme.com:9094
    security:
      protocol: SSL
      ssl:
        truststore: { location: /certs/legacy-ca.p12, password: ${LEGACY_TS_PASS}, type: PKCS12 }
        keystore:   { location: /certs/qa-client.p12, password: ${LEGACY_KS_PASS}, type: PKCS12 }

topics:
  orders:
    cluster: main
    name: ${ENV_PREFIX:dev}.orders.v1
    key:   { format: STRING }
    value: { format: AVRO }
  payments:
    cluster: main
    name: ${ENV_PREFIX:dev}.payments.v1
    key:   { format: STRING }
    value: { format: PROTOBUF, messageType: com.acme.payments.PaymentEvent }
  customers:
    cluster: main
    name: ${ENV_PREFIX:dev}.customers.v1
    value: { format: JSON_SCHEMA }
  audit:
    cluster: legacy
    name: audit-log
    key:   { format: BYTES }
    value: { format: JSON }
  counters:
    cluster: legacy
    name: counters
    key:   { format: STRING }
    value: { format: LONG }

environments:
  local:
    clusters:
      main:
        bootstrapServers: localhost:9092
        security: { protocol: PLAINTEXT }
        schemaRegistry: { url: http://localhost:8081, auth: { type: NONE } }
        admin: { allowDestructive: true }
      legacy:
        bootstrapServers: localhost:9092
        security: { protocol: PLAINTEXT }
  pre:
    clusters:
      main:
        bootstrapServers: kafka-pre.acme.com:9093
```

## 11. Acceso programático a la configuración

```java
KafkaTestKitConfig cfg = kit.config();
cfg.topic("orders").name();                 // "dev.orders.v1"
cfg.cluster("main").bootstrapServers();      // "kafka-dev.acme.com:9093"
cfg.environment();                           // Optional["dev"]
cfg.describe();                              // resumen legible, secretos enmascarados
```
