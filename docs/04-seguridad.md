# 04 · Seguridad

La librería modela la seguridad de cada cluster en el bloque `security` del YAML y la traduce a propiedades nativas de Kafka con `SecurityPropertiesMapper`. Cualquier caso no modelado se puede cubrir con `properties`.

## 1. Modelo

```yaml
security:
  protocol: PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
  ssl:
    truststore: { location, password, type }        # JKS | PKCS12 | PEM
    keystore:   { location, password, keyPassword, type }
    pem:                                            # alternativa a stores
      caCertificates: classpath:certs/ca.pem
      certificateChain: /certs/client.crt
      privateKey: /certs/client.key
      privateKeyPassword: ${KEY_PASS}
    endpointIdentification: https                   # "" para desactivar (NO recomendado)
    protocolVersion: TLSv1.3
  sasl:
    mechanism: PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 | OAUTHBEARER | GSSAPI
    username: ...
    password: ...
    oauth: {...}
    kerberos: {...}
    jaasConfig: ...                                 # escape: JAAS literal, ignora el resto
```

```java
public record SecurityConfig(
        SecurityProtocol protocol,
        Optional<SslConfig> ssl,
        Optional<SaslConfig> sasl) {}

public sealed interface SaslConfig
        permits PlainSasl, ScramSasl, OAuthBearerSasl, GssapiSasl, RawJaasSasl {}
```

## 2. Matriz de validación

| protocol | `ssl` | `sasl` | Notas |
|----------|-------|--------|-------|
| `PLAINTEXT` | prohibido | prohibido | Solo local / Testcontainers |
| `SSL` | truststore opcional (usa CA del JDK si falta); keystore → mTLS | prohibido | |
| `SASL_PLAINTEXT` | prohibido | obligatorio | Warning: credenciales sin cifrar |
| `SASL_SSL` | opcional | obligatorio | Caso más común en cloud |

Errores de combinación se reportan en la validación (ver [03 §9](03-configuracion-yaml.md#9-validación)).

## 3. PLAINTEXT

```yaml
security: { protocol: PLAINTEXT }
```
Genera: `security.protocol=PLAINTEXT`.

## 4. SSL (TLS de servidor)

```yaml
security:
  protocol: SSL
  ssl:
    truststore:
      location: classpath:certs/truststore.jks
      password: ${TS_PASS}
      type: JKS
```

Propiedades generadas:

```properties
security.protocol=SSL
ssl.truststore.location=/tmp/kafka-testkit-1234/truststore.jks
ssl.truststore.password=****
ssl.truststore.type=JKS
ssl.endpoint.identification.algorithm=https
```

> **Recursos de classpath**: Kafka solo acepta rutas de fichero. Cuando `location` empieza por `classpath:`, la librería copia el recurso a un fichero temporal (permisos 600, borrado al cerrar el kit/JVM) y pasa esa ruta. Así los certificados pueden ir dentro del jar de tests.

## 5. mTLS (SSL con certificado de cliente)

```yaml
security:
  protocol: SSL
  ssl:
    truststore: { location: /certs/ca.p12,     password: ${TS_PASS}, type: PKCS12 }
    keystore:   { location: /certs/client.p12, password: ${KS_PASS}, keyPassword: ${KEY_PASS}, type: PKCS12 }
```

Añade:

```properties
ssl.keystore.location=/certs/client.p12
ssl.keystore.password=****
ssl.key.password=****
ssl.keystore.type=PKCS12
```

### Variante PEM (sin keystore)

```yaml
security:
  protocol: SSL
  ssl:
    pem:
      caCertificates: classpath:certs/ca.pem
      certificateChain: /certs/client.crt
      privateKey: /certs/client.key
```

Genera `ssl.truststore.type=PEM`, `ssl.truststore.certificates=<contenido>`, `ssl.keystore.type=PEM`, `ssl.keystore.certificate.chain=<contenido>`, `ssl.keystore.key=<contenido>`. La librería lee los ficheros y pasa el contenido (Kafka lo admite así).

## 6. SASL PLAIN

```yaml
security:
  protocol: SASL_SSL
  sasl:
    mechanism: PLAIN
    username: ${KAFKA_USER}
    password: ${KAFKA_PASSWORD}
```

```properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="qa-user" password="****";
```

Caso típico: **Confluent Cloud** (API key/secret como usuario/contraseña).

## 7. SASL SCRAM-SHA-256 / SCRAM-SHA-512

```yaml
security:
  protocol: SASL_SSL
  sasl:
    mechanism: SCRAM-SHA-512
    username: ${KAFKA_USER}
    password: ${KAFKA_PASSWORD}
  ssl:
    truststore: { location: classpath:certs/ca.jks, password: ${TS_PASS} }
```

```properties
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="qa-user" password="****";
```

Caso típico: **Amazon MSK** con SCRAM, clusters on-premise.

## 8. SASL OAUTHBEARER (OIDC)

```yaml
security:
  protocol: SASL_SSL
  sasl:
    mechanism: OAUTHBEARER
    oauth:
      tokenEndpointUrl: https://idp.acme.com/oauth2/token
      clientId: ${OAUTH_CLIENT_ID}
      clientSecret: ${OAUTH_CLIENT_SECRET}
      scope: kafka
      extensions:                      # opcional, p. ej. Confluent Cloud
        logicalCluster: lkc-abc123
        identityPoolId: pool-xyz
      # jwksEndpointUrl / expectedAudience no aplican al cliente
```

```properties
sasl.mechanism=OAUTHBEARER
sasl.oauthbearer.token.endpoint.url=https://idp.acme.com/oauth2/token
sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="..." clientSecret="****" scope="kafka" extension_logicalCluster="lkc-abc123" extension_identityPoolId="pool-xyz";
```

Opción adicional para entornos que usan un token estático o un handler propio:

```yaml
oauth:
  callbackHandlerClass: com.acme.kafka.MyTokenHandler
  options: { some.key: value }     # se añaden a la línea JAAS
```

**Amazon MSK IAM**: no está modelado en v1; se configura con `properties` y la dependencia `aws-msk-iam-auth`:

```yaml
security:
  protocol: SASL_SSL
  sasl:
    mechanism: AWS_MSK_IAM
    jaasConfig: software.amazon.msk.auth.iam.IAMLoginModule required;
properties:
  sasl.client.callback.handler.class: software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

## 9. SASL GSSAPI (Kerberos)

```yaml
security:
  protocol: SASL_SSL
  sasl:
    mechanism: GSSAPI
    kerberos:
      serviceName: kafka
      principal: qa-user@ACME.COM
      keytab: /etc/security/keytabs/qa-user.keytab
      krb5Conf: /etc/krb5.conf           # opcional
      useTicketCache: false              # true para usar kinit del usuario
```

```properties
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab="/etc/security/keytabs/qa-user.keytab" principal="qa-user@ACME.COM";
```

`krb5Conf` se aplica con `System.setProperty("java.security.krb5.conf", ...)` porque es global de JVM. Si dos clusters indican `krb5Conf` distintos se lanza `ConfigurationException` (limitación de la JVM).

## 10. JAAS literal (escape)

Para cualquier mecanismo no contemplado:

```yaml
security:
  protocol: SASL_SSL
  sasl:
    mechanism: PLAIN
    jaasConfig: >-
      org.apache.kafka.common.security.plain.PlainLoginModule required
      username="${KAFKA_USER}" password="${KAFKA_PASSWORD}";
```

Si hay `jaasConfig`, se ignoran `username`, `password`, `oauth` y `kerberos` (warning si están presentes).

## 11. Schema Registry

Ver [03 §4.1](03-configuracion-yaml.md#41-schemaregistry). Mapeo:

| YAML | Propiedad Confluent |
|------|---------------------|
| `auth.type: BASIC` | `basic.auth.credentials.source=USER_INFO`, `basic.auth.user.info=user:****` |
| `auth.type: USER_INFO_FROM_SASL` | `basic.auth.credentials.source=SASL_INHERIT` |
| `auth.type: BEARER` | `bearer.auth.credentials.source=STATIC_TOKEN`, `bearer.auth.token=****` |
| `ssl.truststore.*` | `schema.registry.ssl.truststore.*` |
| `ssl.keystore.*` | `schema.registry.ssl.keystore.*` |

## 12. Implementación: `SecurityPropertiesMapper`

```java
public final class SecurityPropertiesMapper {

    public Map<String, Object> toKafkaProperties(SecurityConfig sec, SecretFileResolver files) {
        var props = new LinkedHashMap<String, Object>();
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, sec.protocol().name());

        sec.ssl().ifPresent(ssl -> props.putAll(mapSsl(ssl, files)));

        sec.sasl().ifPresent(sasl -> {
            props.put(SaslConfigs.SASL_MECHANISM, sasl.mechanism());
            switch (sasl) {
                case PlainSasl p      -> props.put(SaslConfigs.SASL_JAAS_CONFIG, Jaas.plain(p.username(), p.password()));
                case ScramSasl s      -> props.put(SaslConfigs.SASL_JAAS_CONFIG, Jaas.scram(s.username(), s.password()));
                case OAuthBearerSasl o -> props.putAll(mapOAuth(o));
                case GssapiSasl g     -> props.putAll(mapKerberos(g));
                case RawJaasSasl r    -> props.put(SaslConfigs.SASL_JAAS_CONFIG, r.jaasConfig());
            }
        });
        return props;
    }
}
```

Se testea con un test unitario por mecanismo que compara el mapa generado (ver [13](13-estrategia-de-testing.md)). Las contraseñas con comillas o barras se escapan correctamente en la línea JAAS.

## 13. Buenas prácticas de secretos

- Nunca escribir secretos en claro en el YAML versionado: usar `${VAR}` o `${file:...}`.
- Los valores secretos se guardan internamente como `Secret` (envoltorio con `toString()` = `****`), y solo se "desenvuelven" al construir las propiedades del cliente.
- `kit.config().describe()` y los logs muestran `****`.
- Ficheros temporales derivados de `classpath:` se crean con permisos restringidos y se borran al cerrar.
