# 01 · Visión y alcance

## 1. Problema

En los proyectos con arquitecturas orientadas a eventos, los tests funcionales y de integración necesitan comprobar cosas como:

- "Cuando llamo a `POST /orders`, se publica un `OrderCreated` en el topic `orders` con estos campos."
- "Si publico un `PaymentRequested`, el servicio de pagos lo consume y su consumer group queda sin lag."
- "El topic `audit` tiene 6 particiones y un retention de 7 días."
- "El evento publicado cumple el esquema Avro registrado."

Hoy cada equipo resuelve esto con código ad-hoc: consumidores de `kafka-clients` copiados entre proyectos, `Thread.sleep()` para esperar mensajes, propiedades de seguridad duplicadas, deserialización Avro manual, y utilidades imposibles de usar desde Karate sin mucho código Java de pegamento.

## 2. Objetivo

Construir **una librería Java reutilizable y profesional** que:

1. Encapsule toda la interacción de **test** con Kafka detrás de una API sencilla y estable.
2. Se configure **únicamente con un YAML**: clusters, seguridad, schema registry, topics y formatos.
3. Funcione igual desde **Karate DSL** y desde **cualquier herramienta Java**.
4. Soporte **varios clusters** simultáneamente, con configuraciones de seguridad distintas.
5. Soporte los formatos habituales en entornos corporativos: **String, JSON, bytes, Avro, Protobuf y JSON Schema** con Schema Registry.
6. Elimine las condiciones de carrera típicas de los tests con eventos.

## 3. No-objetivos

| Fuera de alcance | Motivo |
|------------------|--------|
| Construir productores/consumidores para aplicaciones | La librería es para testing; su API prioriza la verificación, no el throughput |
| Procesamiento de streams (Kafka Streams, ksqlDB) | Otra problemática |
| Kafka Connect (gestión de conectores) | Posible extensión futura, no en v1 |
| Monitorización/alertas en producción | Hay herramientas específicas |
| Transacciones exactly-once como funcionalidad de primer nivel | Se permite configurarlo vía `properties`, pero no se diseña API específica en v1 |
| Clientes no-Java (Python, Node) | Karate se usa desde la JVM; no hace falta |

## 4. Usuarios y casos de uso

### 4.1 Perfiles

- **QA / automatizador con Karate**: quiere escribir `* def msg = cap.awaitOne(...)` y `* match msg.value == {...}` sin tocar Java.
- **Desarrollador backend con JUnit**: quiere tests de integración de su microservicio contra Kafka real o embebido.
- **Equipo de plataforma**: quiere verificar configuración de topics, particiones y ACLs en distintos entornos.

### 4.2 Casos de uso principales

| ID | Caso de uso | Documento |
|----|-------------|-----------|
| CU-01 | Verificar que una acción publica un evento concreto | [06](06-capturas-y-esperas.md) |
| CU-02 | Verificar que NO se publica un evento en un intervalo | [06](06-capturas-y-esperas.md) |
| CU-03 | Publicar un evento de entrada para disparar un servicio | [05](05-api-core.md) |
| CU-04 | Esperar a que un consumer group procese todo (lag 0) | [07](07-metadatos-admin-grupos.md) |
| CU-05 | Leer los últimos N mensajes / desde un timestamp | [05](05-api-core.md) |
| CU-06 | Comprobar particiones, réplicas, ISR, configs de un topic | [07](07-metadatos-admin-grupos.md) |
| CU-07 | Crear topics temporales y limpiarlos al terminar | [07](07-metadatos-admin-grupos.md) |
| CU-08 | Verificar el esquema registrado de un topic | [08](08-serdes-y-schema-registry.md) |
| CU-09 | Seguir un flujo de eventos entre varios topics / clusters | [06](06-capturas-y-esperas.md) |
| CU-10 | Ejecutar todo lo anterior contra un Kafka embebido | [10](10-testcontainers.md) |

## 5. Requisitos funcionales

### Configuración
- **RF-01** Toda la configuración de conexión, seguridad, schema registry y topics se define en YAML.
- **RF-02** Se pueden definir N clusters con nombre lógico.
- **RF-03** Se pueden definir topics con nombre lógico, cluster asociado, formato de clave y valor.
- **RF-04** Se soportan variables `${ENV}` y propiedades de sistema con valor por defecto.
- **RF-05** Se soportan entornos (`local`, `dev`, `pre`…) que sobrescriben la configuración base.
- **RF-06** Cualquier propiedad nativa del cliente Kafka se puede pasar tal cual.
- **RF-07** La configuración se valida al cargarla con errores que indican la ruta YAML exacta.

### Producción
- **RF-10** Enviar mensajes con clave, valor, headers, partición y timestamp opcionales.
- **RF-11** El envío es síncrono por defecto y devuelve partición, offset y timestamp.
- **RF-12** Enviar lotes y enviar desde fichero (JSON / NDJSON).
- **RF-13** El valor se puede dar como `Map`, `String`, JSON, `byte[]`, POJO o tipo nativo del formato.

### Lectura y espera
- **RF-20** Leer desde el principio, final, offset, timestamp o "hace X tiempo".
- **RF-21** Leer por particiones concretas, con límite de mensajes y de tiempo.
- **RF-22** Capturar mensajes en segundo plano desde "ahora" y esperar a que aparezca uno que cumpla un filtro.
- **RF-23** Esperar N mensajes, esperar ausencia de mensajes, esperar secuencias en varios topics.
- **RF-24** Filtros por clave, header, JSONPath, campo, partición y predicados arbitrarios.
- **RF-25** En caso de timeout, el error incluye diagnóstico (mensajes vistos, filtro).

### Metadatos, admin y grupos
- **RF-30** Información de cluster, brokers, topics, particiones, líderes, réplicas, ISR, configs.
- **RF-31** Offsets earliest/latest por partición y número aproximado de mensajes.
- **RF-32** Crear, borrar, purgar topics, añadir particiones, cambiar configs.
- **RF-33** Protección configurable contra operaciones destructivas.
- **RF-34** Listar y describir consumer groups, offsets comprometidos, lag; esperar lag objetivo; resetear offsets.

### Serdes
- **RF-40** Formatos STRING, JSON, BYTES, LONG, INTEGER, AVRO, PROTOBUF, JSON_SCHEMA.
- **RF-41** Cualquier mensaje consumido se puede obtener como `Map`/JSON.
- **RF-42** Producir Avro/Protobuf/JSON Schema desde un `Map`.
- **RF-43** Acceso al Schema Registry (subjects, versiones, compatibilidad).
- **RF-44** Extensible con nuevos formatos vía SPI.

### Integraciones
- **RF-50** Fachada para Karate que solo expone tipos nativos de Karate.
- **RF-51** Kafka y Schema Registry embebidos con Testcontainers.

## 6. Requisitos no funcionales

| ID | Requisito | Detalle |
|----|-----------|---------|
| RNF-01 | Java 21 | Se usan records, sealed interfaces, pattern matching |
| RNF-02 | Thread-safety | Uso concurrente desde features Karate en paralelo |
| RNF-03 | No invasiva | Por defecto no se usan consumer groups; no se tocan offsets de grupos reales |
| RNF-04 | Dependencias mínimas | Schema Registry y Testcontainers opcionales; solo `slf4j-api` para logging |
| RNF-05 | Seguridad | Secretos nunca en logs ni en `toString()`; protección de operaciones destructivas |
| RNF-06 | Rendimiento suficiente | Clientes reutilizados; una captura típica detecta un mensaje en < `poll` intervalo tras su llegada |
| RNF-07 | Diagnóstico | Todos los errores dicen qué, dónde (ruta YAML / topic / partición) y cómo arreglarlo |
| RNF-08 | Calidad | Cobertura ≥ 80 % en core, análisis estático, Javadoc en API pública |
| RNF-09 | Compatibilidad | Brokers Kafka ≥ 2.1, Schema Registry Confluent ≥ 7.x |
| RNF-10 | Estabilidad de API | SemVer; paquetes `internal` no forman parte de la API pública |

## 7. Glosario

| Término | Significado |
|---------|-------------|
| **Kit** | Instancia de `KafkaTestKit`: configuración cargada + clientes |
| **Nombre lógico** | Identificador usado en YAML/tests (`orders`) |
| **Nombre físico** | Nombre real del topic en Kafka (`dev.orders.v1`) |
| **Captura** | Consumidor en segundo plano que acumula mensajes para esperarlos (`MessageCapture`) |
| **Filtro** | Predicado sobre `KafkaMessage` |
| **Serde** | Serializador/deserializador de clave o valor |
| **Entorno** | Bloque del YAML que sobrescribe la configuración base |
| **Operación destructiva** | Borrar/purgar topics, borrar grupos, resetear offsets |
| **Lag** | Diferencia entre el último offset del topic y el comprometido por un grupo |
