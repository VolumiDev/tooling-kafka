# 09 · Integración con Karate

## 1. Objetivos del adaptador

- Que un automatizador pueda usar **toda** la librería sin escribir Java.
- Exponer solo tipos que Karate maneja de forma nativa: `Map`, `List`, `String`, números, `boolean`.
- Aceptar filtros como **JSON** (`{ key: 'A1' }`) o como **función JS**.
- Aceptar timeouts como `'20s'`, `'500ms'` o número de milisegundos.
- Ser seguro con la ejecución **en paralelo** de Karate (`Runner.parallel(n)`).

Compatibilidad objetivo: Karate 1.4+ (Java 17+; la librería requiere Java 21).

## 2. Dependencias

```xml
<dependency>
  <groupId>io.github.volumidev</groupId>
  <artifactId>kafka-testkit-karate</artifactId>
  <scope>test</scope>
</dependency>
<!-- si hay Avro/Protobuf/JSON Schema -->
<dependency>
  <groupId>io.github.volumidev</groupId>
  <artifactId>kafka-testkit-schema-registry</artifactId>
  <scope>test</scope>
</dependency>
```

`karate-core` es `provided`: se usa la versión de Karate del proyecto.

## 3. Arranque

### 3.1 En `karate-config.js` (recomendado)

```javascript
function fn() {
  var env = karate.env || 'local';
  var Kafka = Java.type('io.github.volumidev.kafkatestkit.karate.KarateKafka');
  var config = {
    env: env,
    baseUrl: 'http://localhost:8080',
    // una instancia por (fichero, entorno), compartida por todas las features y threads
    kafka: Kafka.load('classpath:kafka-testkit.yml', env)
  };
  return config;
}
```

### 3.2 En una feature

```gherkin
Background:
  * def kafka = Java.type('io.github.volumidev.kafkatestkit.karate.KarateKafka').load(karate.env)
```

`KarateKafka.load(...)` cachea las instancias en un `ConcurrentHashMap` estático por clave `(ubicación, entorno)`, así que llamarlo en cada `Background` no crea clientes nuevos. Se cierran con un shutdown hook o explícitamente con `KarateKafka.closeAll()` (p. ej. en un `@AfterAll` del runner JUnit).

## 4. API de `KarateKafka`

Todos los métodos devuelven `Map`/`List`/primitivos; los mensajes se devuelven con la forma de `KafkaMessage.toMap()` ([05 §3.3](05-api-core.md#33-tomap--forma-canónica-del-mensaje)).

| Método | Devuelve | Descripción |
|--------|----------|-------------|
| `load(env)` / `load(location, env)` | `KarateKafka` | Instancia cacheada |
| `fromKit(kit)` | `KarateKafka` | Envuelve un `KafkaTestKit` ya creado (p. ej. el de Testcontainers) |
| `send(topic, message)` | `Map` (SendResult) | `message = { key, value, headers, partition, timestamp }` |
| `send(topic, key, value)` | `Map` | Atajo |
| `sendAll(topic, list)` | `List<Map>` | |
| `sendFile(topic, path)` | `List<Map>` | Admite `classpath:` y rutas relativas a la feature con `this:` |
| `read(topic, options)` | `List<Map>` | `options = { from: 'beginning'\|'end'\|'-5m'\|ISO\|{partition,offset}, limit, timeout, filter, partitions }` |
| `readLast(topic, n)` | `List<Map>` | |
| `capture(topic)` / `capture(topic, options)` | `KarateCapture` | |
| `captureAll(topics)` | `KarateMultiCapture` | `topics` = lista de nombres |
| `describeTopic(topic)` | `Map` | |
| `topicConfig(topic)` | `Map` | |
| `offsets(topic)` | `Map` | |
| `messageCount(topic)` | `long` | |
| `clusterInfo(cluster)` | `Map` | |
| `listTopics(cluster, regex)` | `List<String>` | |
| `groups(cluster)` | `List<Map>` | |
| `describeGroup(cluster, group)` | `Map` | |
| `groupLag(cluster, group, topic)` | `Map` | |
| `awaitLag(group, topic, maxLag, timeout)` | `Map` | El cluster se infiere del topic |
| `createTopic(cluster, spec)` | `String` | `spec = { name, partitions, replicationFactor, configs }` |
| `createTemporaryTopic(cluster, spec)` | `String` | Devuelve el nombre generado |
| `purgeTopic(topic)` / `deleteTopic(topic)` | — | Sujetos a la política de protección |
| `resetOffsets(group, topic, to)` | — | `to = 'earliest'\|'latest'\|{ partition: offset }\|ISO` |
| `latestSchema(cluster, subject)` | `Map` | |
| `schemaForTopic(topic)` | `Map` | |
| `javaKit()` | `KafkaTestKit` | Escape para usar la API Java directamente |

### `KarateCapture`

| Método | Devuelve |
|--------|----------|
| `awaitOne(filter)` / `awaitOne(filter, timeout)` | `Map` |
| `awaitFirst(timeout)` | `Map` |
| `awaitCount(n, filter, timeout)` | `List<Map>` |
| `awaitExactly(n, filter, settle)` | `List<Map>` |
| `assertNone(filter, window)` | — (falla el paso si aparece) |
| `messages()` / `messages(filter)` | `List<Map>` |
| `size()` / `clear()` / `close()` | |

### `KarateMultiCapture`

| Método | Devuelve |
|--------|----------|
| `topic(name)` | `KarateCapture` del topic indicado |
| `awaitSequence(steps, timeout)` | `List<Map>` — `steps = [ { topic, filter } ]` en orden |
| `messages()` | `List<Map>` de todos los topics, ordenados por timestamp |
| `close()` | Cierra todas las capturas |

## 5. Filtros en Karate

### 5.1 Filtro JSON

Todas las claves presentes se combinan con **AND**:

| Clave | Ejemplo | Equivale a |
|-------|---------|------------|
| `key` | `{ key: 'A1' }` | `Filters.key("A1")` |
| `keyMatches` | `{ keyMatches: 'A\\d+' }` | |
| `headers` | `{ headers: { eventType: 'OrderCreated' } }` | `header(...)` por cada entrada |
| `field` + `equals` | `{ field: 'customer.id', equals: 42 }` | `Filters.field(...)` |
| `jsonPath` + `equals` / `matches` / `exists` | `{ jsonPath: '$.lines[0].sku', equals: 'X1' }` | `Filters.jsonPath(...)` |
| `value` | `{ value: { status: 'CREATED' } }` | `valueContains` (subconjunto profundo) |
| `partition` | `{ partition: 2 }` | |
| `anyOf` | `{ anyOf: [ {key:'A1'}, {key:'A2'} ] }` | OR de sub-filtros |
| `not` | `{ not: { headers: { eventType: 'Deleted' } } }` | |

```gherkin
* def msg = cap.awaitOne({ key: '#(orderId)', headers: { eventType: 'OrderCreated' }, value: { status: 'CREATED' } }, '20s')
```

### 5.2 Filtro función JS

La función recibe el mensaje en forma de Map:

```gherkin
* def isBig = function(m){ return m.value.amount > 100 && m.headers.source == 'web' }
* def msg = cap.awaitOne(isBig, '20s')
```

> **Nota técnica**: el motor JS de Karate (GraalJS) no permite ejecutar una función desde otro hilo. Por eso **los filtros se evalúan siempre en el hilo del test** (el que llama a `awaitOne`), nunca en el worker de la captura. El worker solo llena el buffer; `await*` recorre el buffer en el hilo llamante cada vez que recibe una señal. Por la misma razón `capture(topic, { filter })` solo acepta filtros JSON, no funciones.

### 5.3 Usar `match` de Karate como filtro

Para la máxima expresividad, filtro `match` con la sintaxis de marcadores de Karate:

```gherkin
* def msg = cap.awaitOne({ match: { orderId: '#(orderId)', amount: '#number', lines: '#[2]' } }, '20s')
```

Se implementa invocando el motor `Match` de Karate (`com.intuit.karate.Match`) en el hilo llamante, con semántica `contains`.

## 6. Ejemplos de features

### 6.1 Verificar evento tras llamada REST

```gherkin
Feature: Pedidos publican eventos

Background:
  * url baseUrl
  * def orderId = java.util.UUID.randomUUID() + ''

Scenario: crear un pedido publica OrderCreated
  * def cap = kafka.capture('orders')
  Given path 'orders'
  And request { orderId: '#(orderId)', amount: 10, currency: 'EUR' }
  When method post
  Then status 201
  * def msg = cap.awaitOne({ key: '#(orderId)' }, '20s')
  * match msg.headers.eventType == 'OrderCreated'
  * match msg.value == { orderId: '#(orderId)', amount: 10, currency: 'EUR', status: 'CREATED', createdAt: '#string' }
  * match msg.partition == '#number'
  * cap.close()
```

### 6.2 Publicar evento de entrada y esperar consumo

```gherkin
Scenario: el servicio de pagos procesa PaymentRequested
  * def payment = read('classpath:data/payment-requested.json')
  * set payment.paymentId = orderId
  * def res = kafka.send('payments-requests', { key: '#(orderId)', value: '#(payment)', headers: { eventType: 'PaymentRequested' } })
  * match res.offset == '#number'
  * kafka.awaitLag('payments-service', 'payments-requests', 0, '60s')
  Given path 'payments', orderId
  When method get
  Then status 200
  And match response.status == 'AUTHORIZED'
```

### 6.3 Flujo multi-topic

```gherkin
Scenario: checkout completo
  * def cap = kafka.captureAll(['orders', 'payments', 'audit'])
  Given path 'checkout'
  And request { cartId: '#(orderId)' }
  When method post
  Then status 202
  * def seq = cap.awaitSequence([
      { topic: 'orders',   filter: { value: { cartId: '#(orderId)', status: 'CREATED' } } },
      { topic: 'payments', filter: { value: { cartId: '#(orderId)', status: 'AUTHORIZED' } } },
      { topic: 'orders',   filter: { value: { cartId: '#(orderId)', status: 'PAID' } } }
    ], '30s')
  * match seq == '#[3]'
  * def audits = cap.topic('audit').awaitCount(3, { field: 'correlationId', equals: '#(orderId)' }, '10s')
  * cap.close()
```

### 6.4 No debe publicarse nada

```gherkin
Scenario: un pedido inválido no publica eventos
  * def cap = kafka.capture('orders')
  Given path 'orders'
  And request { orderId: '#(orderId)', amount: -1 }
  When method post
  Then status 400
  * cap.assertNone({ key: '#(orderId)' }, '5s')
```

### 6.5 Metadatos e infraestructura

```gherkin
Scenario: el topic orders cumple la definición
  * def t = kafka.describeTopic('orders')
  * match t.partitionCount == 6
  * match t.replicationFactor == 3
  * def cfg = kafka.topicConfig('orders')
  * match cfg contains { 'cleanup.policy': 'delete', 'retention.ms': '604800000' }
```

### 6.6 Topic temporal

```gherkin
Scenario: prueba aislada con topic temporal
  * def name = kafka.createTemporaryTopic('main', { partitions: 3 })
  * kafka.send(name, { key: 'k1', value: { a: 1 } })
  * def msgs = kafka.read(name, { from: 'beginning' })
  * match msgs == '#[1]'
```

Nota: los topics temporales/dinámicos usan el formato por defecto (`defaults.serde`), o `{ format: {...} }` en `spec`.

## 7. Runner con ejecución paralela

```java
class KafkaFeaturesTest {

    @Test
    void runAll() {
        Results r = Runner.path("classpath:features")
                .karateEnv(System.getProperty("karate.env", "local"))
                .parallel(8);
        assertEquals(0, r.getFailCount(), r.getErrorMessages());
    }

    @AfterAll
    static void closeKafka() { KarateKafka.closeAll(); }
}
```

Por qué es seguro en paralelo:
- Instancia de kit compartida y thread-safe; producers compartidos (thread-safe en Kafka).
- Cada `capture` tiene su propio consumidor y su hilo virtual.
- Los filtros JS se ejecutan en el hilo del escenario que los definió.
- Cada escenario debe filtrar por un identificador único (ver buenas prácticas de [06 §9](06-capturas-y-esperas.md#9-buenas-prácticas)).

## 8. Errores en Karate

Las excepciones de la librería se propagan como fallo del paso, con el mensaje de diagnóstico completo en el informe HTML de Karate. Además, `KarateKafka` llama a `karate.log` (vía `ScenarioEngine` cuando está disponible) para dejar en el informe una línea por mensaje enviado/recibido:

```
kafka → orders[2]@1045 key=A1
kafka ← orders[2]@1046 key=A1 eventType=OrderCreated (esperado 1.3s)
```
