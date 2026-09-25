# 13 · Estrategia de testing de la librería

Una librería de testing tiene que ser especialmente fiable: un falso positivo o un test inestable en ella se multiplica en todos los proyectos que la usan.

## 1. Pirámide

```
            ┌──────────────────────┐
            │  Karate e2e (it)     │  features reales contra Kafka embebido, en paralelo
            ├──────────────────────┤
            │  Integración (it)    │  JUnit + Testcontainers: Kafka, SR, seguridad
            ├──────────────────────┤
            │  Unitarios (módulos) │  config, mapeos, filtros, conversiones, diagnósticos
            └──────────────────────┘
```

| Nivel | Dónde | Plugin | Requiere Docker |
|-------|-------|--------|-----------------|
| Unitarios | cada módulo, `*Test` | surefire | No |
| Integración | `kafka-testkit-it`, `*IT` | failsafe | Sí |
| Karate | `kafka-testkit-it/src/test/resources/features` | failsafe (runner JUnit) | Sí |

## 2. Tests unitarios (sin Kafka)

### Configuración
- Carga de YAML válido completo → modelo esperado (test *golden*).
- Cada regla de validación de [03 §9](03-configuracion-yaml.md#9-validación) tiene su test con el mensaje exacto.
- Interpolación: variables, defaults, anidados, escapes, `${file:}`, variables ausentes.
- Merge de entornos: mapas, listas, reemplazo de `security` al cambiar `protocol`, `null` explícito, entorno inexistente.
- `include` de varios ficheros y orden de precedencia.
- Precedencia de propiedades nativas ([03 §8](03-configuracion-yaml.md#8-precedencia-de-propiedades-nativas-de-kafka)).

### Seguridad
Un test parametrizado por mecanismo que compara el mapa de propiedades generado:

```java
@ParameterizedTest(name = "{0}")
@MethodSource("securityCases")
void maps_security_to_kafka_properties(String name, String yaml, Map<String, Object> expected) {
    var sec = TestYaml.parse(yaml, SecurityConfig.class);
    assertThat(mapper.toKafkaProperties(sec, files)).containsExactlyInAnyOrderEntriesOf(expected);
}

static Stream<Arguments> securityCases() {
    return Stream.of(
        arguments("scram-512", """
            protocol: SASL_SSL
            sasl: { mechanism: SCRAM-SHA-512, username: u, password: 'p"x' }
            """, Map.of(
                "security.protocol", "SASL_SSL",
                "sasl.mechanism", "SCRAM-SHA-512",
                "sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"u\" password=\"p\\\"x\";")),
        // plain, oauth, gssapi, mtls, pem, raw jaas...
    );
}
```

- Secretos: ningún `toString()`, mensaje de excepción ni log contiene la contraseña (test con un appender en memoria).

### Filtros y diagnóstico
- Cada `Filters.*` con casos positivos, negativos, nulos y tipos numéricos mezclados.
- `describe()` de filtros compuestos.
- `TimeoutDiagnostics`: salida esperada con coincidencias parciales (test *golden* de texto).

### Captura (sin Kafka)
- `CaptureBuffer` con `MockConsumer` de `kafka-clients`: señalización, `consuming`, `maxBuffered`, fallo del worker despierta a los `await`.
- Uso de un `Clock` y tiempos inyectables para no depender del reloj real en timeouts.

### Serdes
- JSON/String/Bytes/Number: ida y vuelta.
- `AvroMapConverter`: tabla de [08 §4.4](08-serdes-y-schema-registry.md#44-avro) completa, uniones, logical types, errores con ruta de campo.
- `ProtobufMapConverter`: `oneof`, `repeated`, `map`, `int64`, `Timestamp`, nombres de campo.
- Serdes SR con `MockSchemaRegistryClient` de Confluent (sin contenedor).

### Karate (unitario)
- Conversión de filtros JSON a `MessageFilter`.
- `KarateDurations`: `'20s'`, `'500ms'`, `20000`, errores.

## 3. Tests de integración (Testcontainers)

Contenedores compartidos por toda la suite (singleton) para que sea rápida.

| Suite | Qué verifica |
|-------|--------------|
| `ProduceReadIT` | Envío y lectura en todos los formatos básicos; headers; partición explícita; timestamps; `readLast`; `fromTimestamp` |
| `CaptureIT` | No se pierde un mensaje enviado justo tras `capture()` (repetido 200 veces); `awaitCount`; `assertNone`; particiones añadidas en caliente; timeouts con diagnóstico |
| `MultiClusterIT` | Dos contenedores Kafka distintos; captura multi-topic en ambos; `awaitSequence` |
| `SchemaRegistryIT` | Avro/Protobuf/JSON Schema: producir desde Map, leer a Map, `schemaFile`, `version`, compatibilidad; interoperabilidad con un productor Confluent "real" |
| `AdminIT` | Crear/borrar/purgar/añadir particiones; topics temporales borrados al cerrar; política de protección |
| `ConsumerGroupIT` | Arranca un consumidor "servicio" en un hilo con `group.id`; `lag`, `awaitLag`, `describe`, `resetOffsets` con grupo vacío/activo |
| `SecurityScramIT` | Kafka con SASL_SSL/SCRAM (certificados generados en el build) |
| `SecurityMtlsIT` | Kafka con SSL + client auth required |
| `ConnectivityErrorsIT` | Mensajes clasificados: broker inexistente, credenciales malas, truststore incorrecto |
| `ConcurrencyIT` | 32 hilos virtuales produciendo y capturando en paralelo sobre el mismo kit; sin fugas de consumidores ni mensajes cruzados |

Certificados de test: generados en la fase `generate-test-resources` con un pequeño helper Java (BouncyCastle) o `keytool` vía `exec-maven-plugin`; nunca se versionan claves privadas.

## 4. Suite Karate

`kafka-testkit-it/src/test/resources/features/`:

```
features/
├── produce-consume.feature
├── capture-await.feature
├── avro-protobuf.feature
├── metadata.feature
├── consumer-groups.feature
├── multi-topic-flow.feature
└── parallel-safety.feature      # 20 escenarios idénticos con ids únicos
```

Runner con `parallel(8)` y `karate.env=embedded`. Un pequeño **servicio simulado** (un consumidor-productor en Java arrancado en `@BeforeAll`) actúa de "sistema bajo prueba": lee de `commands`, publica en `events`, para poder probar flujos y lag de forma realista.

## 5. Estabilidad (anti-flaky)

- Ningún `Thread.sleep()` en tests de la librería salvo para verificar `assertNone`.
- Timeouts generosos en CI (`KAFKA_TESTKIT_TIMEOUT_SCALE=2` multiplica todos los timeouts de test).
- Los tests de "no se pierden mensajes" se repiten N veces (`@RepeatedTest`) para detectar carreras.
- Cualquier test inestable detectado se trata como bug de la librería, no se marca `@Disabled`.

## 6. Criterios de calidad

| Métrica | Umbral |
|---------|--------|
| Cobertura de líneas en `core` | ≥ 80 % |
| Cobertura en `schema-registry` | ≥ 75 % |
| SpotBugs | 0 issues de prioridad alta/media |
| Checkstyle | 0 errores |
| Javadoc | toda clase/método público documentado |
| Build completo en CI | < 10 min |
