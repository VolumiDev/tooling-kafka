# 05 · API core: producir y leer

## 1. Punto de entrada: `KafkaTestKit`

```java
public interface KafkaTestKit extends AutoCloseable {

    // --- Creación ---
    static KafkaTestKit load();                                   // resolución por defecto (ver 03 §1)
    static KafkaTestKit load(String configLocation);
    static KafkaTestKit load(String configLocation, String environment);
    static KafkaTestKit load(List<String> configLocations, String environment);
    static KafkaTestKitBuilder builder();

    // --- Navegación ---
    TopicClient topic(String logicalName);
    ClusterClient cluster(String logicalName);
    SchemaRegistryClient schemaRegistry(String clusterName);      // requiere módulo schema-registry

    // --- Capturas multi-topic ---
    MultiTopicCapture capture(String... logicalTopicNames);

    // --- Utilidades ---
    KafkaTestKitConfig config();
    void verifyConnectivity();                                    // ping a todos los clusters usados por topics declarados
    @Override void close();
}
```

Builder para casos avanzados:

```java
KafkaTestKit kit = KafkaTestKit.builder()
        .config("classpath:kafka-testkit.yml")
        .environment("local")
        .override("clusters.main.bootstrapServers", "localhost:29092")
        .defaultAwaitTimeout(Duration.ofSeconds(10))
        .registerShutdownHook(true)
        .clock(Clock.systemUTC())            // inyectable para tests de la librería
        .build();
```

### Uso típico con JUnit 5

```java
class OrderEventsIT {

    static KafkaTestKit kit;

    @BeforeAll static void init()    { kit = KafkaTestKit.load(); }
    @AfterAll  static void cleanup() { kit.close(); }

    @Test
    void creating_an_order_publishes_OrderCreated() {
        try (var capture = kit.topic("orders").capture()) {
            api.createOrder("A1", 10);

            KafkaMessage msg = capture.awaitOne(Filters.key("A1"));
            assertThat(msg.valueAsMap())
                    .containsEntry("orderId", "A1")
                    .containsEntry("status", "CREATED");
            assertThat(msg.header("eventType")).contains("OrderCreated");
        }
    }
}
```

## 2. `ClusterClient`

```java
public interface ClusterClient {
    String name();
    ClusterConfig config();

    MetadataClient metadata();             // ver 07
    TopicAdmin admin();                    // ver 07
    ConsumerGroupClient groups();          // ver 07

    TopicClient topic(String physicalName, TopicOptions options);   // topic no declarado en YAML
    void ping();                           // describeCluster con timeout.request; lanza si no responde
}
```

`TopicOptions` define formatos al vuelo:

```java
TopicOptions.json();                                   // key STRING, value JSON
TopicOptions.of(SerdeFormat.STRING, SerdeFormat.AVRO).subject("x-value");
TopicOptions.bytes();
```

## 3. Modelo de mensajes

### 3.1 `KafkaMessage` (entrada: lo que se lee)

```java
public record KafkaMessage(
        String topic,                 // nombre físico
        String logicalTopic,          // nombre lógico (null si topic dinámico)
        int partition,
        long offset,
        Instant timestamp,
        TimestampType timestampType,  // CREATE_TIME | LOG_APPEND_TIME
        Object key,                   // deserializado según formato
        Object value,                 // deserializado según formato (null = tombstone)
        Headers headers,
        Optional<Integer> keySchemaId,
        Optional<Integer> valueSchemaId,
        byte[] rawKey,
        byte[] rawValue) {

    // Conversiones
    Map<String, Object> valueAsMap();             // JSON/Avro/Protobuf/JSON Schema -> Map
    String valueAsString();                       // representación textual (JSON para estructurados)
    String valueAsJson();                         // JSON canónico
    <T> T valueAs(Class<T> type);                 // Jackson: Map -> POJO
    Object keyAsObject();                         // Map si estructurada, si no el valor
    String keyAsString();

    // Headers
    Optional<String> header(String name);         // último valor, decodificado UTF-8
    List<String> headerValues(String name);

    // Utilidades
    boolean isTombstone();
    Map<String, Object> toMap();                  // mensaje completo (ver §3.3)
    String summary();                             // "orders[2]@1045 key=A1 value={orderId=A1,...}" truncado
}
```

`Headers`: vista inmutable con `Map<String,String> asMap()` (último valor por nombre, UTF-8), `List<Header> all()` y `byte[] raw(name)`.

### 3.2 `OutgoingMessage` (salida: lo que se envía)

```java
OutgoingMessage msg = OutgoingMessage.builder()
        .key("A1")
        .value(Map.of("orderId", "A1", "amount", 10))
        .header("eventType", "OrderCreated")
        .header("traceId", UUID.randomUUID().toString())
        .partition(2)                              // opcional
        .timestamp(Instant.parse("2026-01-01T00:00:00Z"))  // opcional
        .build();

// atajos
OutgoingMessage.of("A1", payload);
OutgoingMessage.tombstone("A1");
```

Tipos aceptados como `value` según formato:

| Formato | Tipos aceptados |
|---------|-----------------|
| STRING | `String`, cualquier objeto (`toString()`) |
| JSON | `Map`, `List`, POJO, `String` con JSON válido, `JsonNode` |
| BYTES | `byte[]`, `ByteBuffer`, `String` en Base64 con prefijo `base64:` |
| LONG / INTEGER | `Number`, `String` numérico |
| AVRO | `Map`, `String` JSON, `GenericRecord`, `SpecificRecord`, POJO (vía Map) |
| PROTOBUF | `Map`, `String` JSON, `Message` (generado o `DynamicMessage`) |
| JSON_SCHEMA | `Map`, POJO, `String` JSON, `JsonNode` |

### 3.3 `toMap()` — forma canónica del mensaje

Es la forma que ve Karate y la que se usa en diagnósticos:

```json
{
  "topic": "dev.orders.v1",
  "logicalTopic": "orders",
  "partition": 2,
  "offset": 1045,
  "timestamp": "2026-09-25T10:15:30.123Z",
  "timestampType": "CREATE_TIME",
  "key": "A1",
  "value": { "orderId": "A1", "amount": 10, "status": "CREATED" },
  "headers": { "eventType": "OrderCreated", "traceId": "5f1..." },
  "keySchemaId": null,
  "valueSchemaId": 42
}
```

### 3.4 `SendResult`

```java
public record SendResult(String topic, int partition, long offset, Instant timestamp,
                         Optional<Integer> valueSchemaId) {
    Map<String, Object> toMap();
}
```

## 4. `TopicClient` — producir

```java
public interface TopicClient {
    String logicalName();
    String physicalName();
    TopicConfig config();

    // --- Producir ---
    SendResult send(OutgoingMessage message);
    SendResult send(Object key, Object value);
    SendResult send(Object key, Object value, Map<String, String> headers);
    List<SendResult> sendAll(List<OutgoingMessage> messages);       // en orden, síncrono
    List<SendResult> sendFromFile(String location);                  // JSON array o NDJSON
    CompletableFuture<SendResult> sendAsync(OutgoingMessage message);
    void flush();

    // --- Leer ---
    List<KafkaMessage> read(ReadSpec spec);
    List<KafkaMessage> readLast(int n);
    Optional<KafkaMessage> readAt(int partition, long offset);
    Stream<KafkaMessage> stream(ReadSpec spec);                      // lectura perezosa, cerrar el stream

    // --- Capturar ---
    MessageCapture capture();
    MessageCapture capture(CaptureOptions options);
}
```

### 4.1 Semántica de `send`

1. Se aplican `headers.defaults` del topic (los headers del mensaje ganan).
2. Se serializa con el serde del topic. Errores de serialización → `SerdeException` con el campo concreto (p. ej. `Avro: campo 'amount' esperaba int, recibido String "diez"`).
3. `producer.send(record).get(timeouts.request)`.
4. Log DEBUG: `→ orders[2]@1045 key=A1 (312 bytes)`.
5. Devuelve `SendResult`.

### 4.2 Envío desde fichero

`sendFromFile` admite dos formatos, detectados por contenido:

```json
[
  { "key": "A1", "value": { "orderId": "A1", "amount": 10 }, "headers": { "eventType": "OrderCreated" } },
  { "key": "A2", "value": { "orderId": "A2", "amount": 20 } }
]
```

NDJSON (un mensaje por línea, mismo esquema de objeto). Se admiten plantillas sencillas `#{uuid}`, `#{now}`, `#{random.int(1,100)}` en valores string, útiles para datos únicos por ejecución.

## 5. `TopicClient` — leer

### 5.1 `ReadSpec`

```java
ReadSpec spec = ReadSpec.fromBeginning()          // posición inicial
        .partitions(0, 1)                         // defecto: todas
        .limit(100)                               // máx. mensajes (defecto 1000)
        .timeout(Duration.ofSeconds(5))           // máx. tiempo (defecto timeouts.read)
        .until(ReadSpec.Until.END_AT_START)       // parar al alcanzar el final actual (defecto)
        .filter(Filters.header("eventType", "OrderCreated"));
```

Posiciones iniciales:

| Factoría | Significado |
|----------|-------------|
| `fromBeginning()` | Offset earliest de cada partición |
| `fromEnd()` | Offset latest (solo tiene sentido con `until(TIMEOUT)`) |
| `fromOffset(partition, offset)` | Offset concreto en una partición |
| `fromOffsets(Map<Integer,Long>)` | Offset por partición |
| `fromTimestamp(Instant)` | `offsetsForTimes` por partición |
| `fromAgo(Duration)` | `fromTimestamp(now - d)` |
| `lastN(n)` | `end - n` por partición, luego ordena por timestamp y devuelve los n últimos globales |

Condiciones de parada (`Until`):

| Valor | Comportamiento |
|-------|----------------|
| `END_AT_START` (defecto) | Se capturan los offsets end al empezar; se para al alcanzarlos en todas las particiones. Determinista. |
| `TIMEOUT` | Se lee hasta agotar `timeout` (ve mensajes que lleguen durante la lectura) |
| `LIMIT` | Se para al alcanzar `limit` |

Siempre se para en el primero de: condición `Until`, `limit` o `timeout`. Si se alcanza `timeout` con `END_AT_START` sin llegar al final, se devuelve lo leído y se registra un warning.

Orden del resultado: por timestamp y, en empate, por (partición, offset). Dentro de una partición el orden es siempre el de offset.

### 5.2 Implementación de la lectura

```java
List<KafkaMessage> read(ReadSpec spec) {
    try (var consumer = consumerFactory.create(topic, /*groupId*/ spec.groupId())) {
        var partitions = resolvePartitions(consumer, spec);     // partitionsFor + filtro
        consumer.assign(partitions);
        var endOffsets = consumer.endOffsets(partitions);
        seekToStart(consumer, partitions, spec.from());         // seekToBeginning / seek / offsetsForTimes
        var out = new ArrayList<KafkaMessage>();
        var deadline = clock.instant().plus(spec.timeout());
        while (!done(consumer, endOffsets, out, spec, deadline)) {
            for (var rec : consumer.poll(pollInterval)) {
                var msg = messageFactory.from(rec, topicConfig);
                if (spec.filter().test(msg)) out.add(msg);
            }
        }
        return sort(out);
    }
}
```

Errores de deserialización durante la lectura: por defecto el mensaje se incluye con `value = null` y un `DeserializationProblem` accesible con `msg.problem()`, en vez de abortar toda la lectura (un mensaje corrupto no debe impedir ver el resto). Configurable con `ReadSpec.failOnDeserializationError(true)`.

### 5.3 Lectura con consumer group

```java
kit.topic("orders").read(ReadSpec.fromBeginning().withGroup("qa-reader").commit(true));
```

Usa `subscribe()` y espera la asignación. Pensado solo para probar comportamientos de grupo; desaconsejado para verificación normal.

## 6. Ejemplos rápidos

```java
// Producir un evento de entrada y verificar el de salida en otro cluster
try (var audit = kit.topic("audit").capture()) {
    kit.topic("orders").send("A1", Map.of("orderId", "A1", "amount", 10));
    audit.awaitOne(Filters.field("entityId", "A1").and(Filters.field("action", "ORDER_RECEIVED")));
}

// Últimos 5 mensajes de un topic
kit.topic("payments").readLast(5).forEach(m -> System.out.println(m.summary()));

// Mensajes de la última hora con cierto header
var msgs = kit.topic("orders").read(ReadSpec.fromAgo(Duration.ofHours(1))
        .filter(Filters.header("source", "web")));

// Topic no declarado en el YAML
kit.cluster("main").topic("tmp.debug", TopicOptions.json()).send("k", Map.of("x", 1));
```
