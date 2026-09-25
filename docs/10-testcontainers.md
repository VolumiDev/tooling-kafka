# 10 · Testcontainers (`kafka-testkit-testcontainers`)

Permite ejecutar los mismos tests contra un Kafka (y Schema Registry) efímero en Docker, sin cambiar el código de test ni el YAML principal.

## 1. Qué levanta

| Componente | Imagen por defecto | Notas |
|------------|--------------------|-------|
| Kafka | `apache/kafka:<versión alineada con kafka-clients>` | KRaft, un nodo, `PLAINTEXT` |
| Schema Registry | `confluentinc/cp-schema-registry:<versión Confluent>` | Solo si algún topic usa AVRO/PROTOBUF/JSON_SCHEMA o se pide explícitamente |
| Kafka con seguridad (opcional) | `apache/kafka` con SASL_SSL/SCRAM o mTLS | Usado sobre todo por los tests de la propia librería |

Las imágenes se pueden cambiar (p. ej. registro interno de la empresa):

```yaml
# kafka-testkit.yml
testcontainers:
  kafkaImage: registry.acme.com/mirror/apache/kafka:4.1.0
  schemaRegistryImage: registry.acme.com/mirror/confluentinc/cp-schema-registry:8.0.0
  reuse: true                       # Testcontainers reuse (requiere testcontainers.reuse.enable=true)
  autoCreateTopics: true            # crea los topics del YAML con su 'definition'
  clusters: [ main, legacy ]        # clusters lógicos que se redirigen al contenedor
```

Los clusters listados se **redirigen todos al mismo contenedor** (suficiente para tests funcionales). Si se necesitan contenedores separados: `clusters: { main: own, legacy: own }`.

## 2. API

```java
public final class EmbeddedKafka implements AutoCloseable {

    public static EmbeddedKafka start();                         // lee bloque testcontainers del YAML por defecto
    public static EmbeddedKafka start(EmbeddedKafkaOptions options);

    public String bootstrapServers();                            // del cluster por defecto
    public String bootstrapServers(String logicalCluster);
    public Optional<String> schemaRegistryUrl();

    /** Crea un kit con la configuración del YAML + overrides apuntando a los contenedores. */
    public KafkaTestKit kit();
    public KafkaTestKit kit(String configLocation);

    /** Overrides en forma de mapa, por si se quiere construir el kit a mano o pasar a otra herramienta. */
    public Map<String, String> overrides();

    @Override public void close();
}
```

```java
EmbeddedKafkaOptions.defaults()
        .clusters("main", "legacy")
        .withSchemaRegistry(true)
        .autoCreateTopics(true)
        .kafkaImage("apache/kafka:4.1.0")
        .brokerProperty("auto.create.topics.enable", "false")
        .security(EmbeddedSecurity.saslScram("admin", "admin-secret"));   // opcional
```

Internamente `kit()` equivale a:

```java
KafkaTestKit.builder()
        .config(location)
        .environment(env)
        .override("clusters.main.bootstrapServers", kafka.getBootstrapServers())
        .override("clusters.main.security.protocol", "PLAINTEXT")
        .override("clusters.main.schemaRegistry.url", srUrl)
        .override("clusters.main.admin.allowDestructive", "true")
        .build();
```

## 3. Extensión JUnit 5

```java
@KafkaTestKitContainers(clusters = {"main"}, schemaRegistry = true)
class OrderServiceIT {

    @InjectKafkaTestKit
    KafkaTestKit kit;                 // listo para usar, apuntando a los contenedores

    @Test
    void publishes_order_created() {
        try (var cap = kit.topic("orders").capture()) {
            kit.topic("commands").send("A1", Map.of("type", "CreateOrder"));
            cap.awaitOne(Filters.key("A1"));
        }
    }
}
```

- Ciclo de vida: contenedores **por clase** por defecto; `scope = Scope.SUITE` los comparte entre todas las clases (singleton con shutdown hook).
- Si el servicio bajo prueba se arranca en el mismo test (Spring Boot, Quarkus…), `EmbeddedKafka.overrides()` o `bootstrapServers()` se pasan a su configuración (p. ej. `@DynamicPropertySource`).

```java
@DynamicPropertySource
static void kafkaProps(DynamicPropertyRegistry r) {
    r.add("spring.kafka.bootstrap-servers", () -> embedded.bootstrapServers());
    r.add("spring.kafka.properties.schema.registry.url", () -> embedded.schemaRegistryUrl().orElseThrow());
}
```

## 4. Uso desde Karate

```javascript
// karate-config.js
function fn() {
  var env = karate.env || 'local';
  var Kafka = Java.type('io.github.volumidev.kafkatestkit.karate.KarateKafka');
  var kafka;
  if (env == 'embedded') {
    // callSingle garantiza un único arranque aunque haya ejecución paralela
    var emb = karate.callSingle('classpath:embedded-kafka.js');
    kafka = Kafka.fromKit(emb.kit);
  } else {
    kafka = Kafka.load(env);
  }
  return { env: env, kafka: kafka };
}
```

```javascript
// embedded-kafka.js
function fn() {
  var Embedded = Java.type('io.github.volumidev.kafkatestkit.testcontainers.EmbeddedKafka');
  var emb = Embedded.shared();          // singleton de JVM, se cierra con shutdown hook
  return { kit: emb.kit(), bootstrap: emb.bootstrapServers() };
}
```

`EmbeddedKafka.shared()` devuelve un singleton de JVM thread-safe, pensado precisamente para Karate.

## 5. Creación automática de topics

Con `autoCreateTopics: true`, al arrancar se recorren los `topics:` de los clusters redirigidos y se crean con su `definition` (o 1 partición / RF 1 si no la tienen). El `replicationFactor` se limita a 1 (un solo broker) con un log INFO. Para Avro/Protobuf/JSON Schema con `schemaFile`, se registran los esquemas en el registry embebido (en el contenedor sí se permite `auto.register`).

## 6. Requisitos y limitaciones

- Docker (o compatible: Podman, Colima, Rancher Desktop) accesible para Testcontainers.
- En CI: usar el servicio Docker del runner o Testcontainers Cloud.
- Kerberos y OAUTHBEARER no se emulan en contenedor en v1 (se prueban con tests unitarios de mapeo de propiedades).
