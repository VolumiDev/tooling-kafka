/*
 * Copyright 2026 VolumiDev
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.volumidev.kafkatestkit.config.internal;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.volumidev.kafkatestkit.config.AdminPolicy;
import io.github.volumidev.kafkatestkit.config.CaptureDefaults;
import io.github.volumidev.kafkatestkit.config.ClusterConfig;
import io.github.volumidev.kafkatestkit.config.DefaultsConfig;
import io.github.volumidev.kafkatestkit.config.GssapiSasl;
import io.github.volumidev.kafkatestkit.config.KafkaTestKitConfig;
import io.github.volumidev.kafkatestkit.config.LoggingConfig;
import io.github.volumidev.kafkatestkit.config.OAuthBearerSasl;
import io.github.volumidev.kafkatestkit.config.PayloadLogging;
import io.github.volumidev.kafkatestkit.config.PemConfig;
import io.github.volumidev.kafkatestkit.config.PlainSasl;
import io.github.volumidev.kafkatestkit.config.RawJaasSasl;
import io.github.volumidev.kafkatestkit.config.SaslConfig;
import io.github.volumidev.kafkatestkit.config.SchemaRegistryAuth;
import io.github.volumidev.kafkatestkit.config.SchemaRegistryAuthType;
import io.github.volumidev.kafkatestkit.config.SchemaRegistryConfig;
import io.github.volumidev.kafkatestkit.config.ScramMechanism;
import io.github.volumidev.kafkatestkit.config.ScramSasl;
import io.github.volumidev.kafkatestkit.config.Secret;
import io.github.volumidev.kafkatestkit.config.SecurityConfig;
import io.github.volumidev.kafkatestkit.config.SecurityProtocol;
import io.github.volumidev.kafkatestkit.config.SerdeConfig;
import io.github.volumidev.kafkatestkit.config.SerdeDefaults;
import io.github.volumidev.kafkatestkit.config.SerdeFormat;
import io.github.volumidev.kafkatestkit.config.SslConfig;
import io.github.volumidev.kafkatestkit.config.StoreConfig;
import io.github.volumidev.kafkatestkit.config.StoreType;
import io.github.volumidev.kafkatestkit.config.SubjectNameStrategy;
import io.github.volumidev.kafkatestkit.config.TimeoutsConfig;
import io.github.volumidev.kafkatestkit.config.TopicConfig;
import io.github.volumidev.kafkatestkit.config.TopicDefinition;
import io.github.volumidev.kafkatestkit.exception.ConfigurationException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Convierte el árbol de configuración ya validado en los records fuertemente tipados del modelo
 * público. Se asume que el árbol ya pasó {@link ConfigValidator}: aquí no se vuelven a comprobar
 * claves desconocidas ni combinaciones inválidas.
 */
public final class ConfigTreeToRecordsMapper {

    private ConfigTreeToRecordsMapper() {}

    /**
     * Construye la configuración completa a partir del árbol validado.
     *
     * @param root árbol raíz ya interpolado, fusionado y validado
     * @param environment entorno seleccionado, si alguno
     * @return la configuración tipada
     */
    public static KafkaTestKitConfig map(JsonNode root, Optional<String> environment) {
        var defaults = mapDefaults(root.path("defaults"));
        var clusters = new LinkedHashMap<String, ClusterConfig>();
        root.path("clusters")
                .properties()
                .forEach(e -> clusters.put(e.getKey(), mapCluster(e.getKey(), e.getValue())));
        var topics = new LinkedHashMap<String, TopicConfig>();
        root.path("topics")
                .properties()
                .forEach(
                        e ->
                                topics.put(
                                        e.getKey(),
                                        mapTopic(e.getKey(), e.getValue(), defaults.serde())));
        return new KafkaTestKitConfig(
                environment, defaults, Map.copyOf(clusters), Map.copyOf(topics));
    }

    private static DefaultsConfig mapDefaults(JsonNode node) {
        var timeouts =
                new TimeoutsConfig(
                        duration(node.path("timeouts").path("await"), "30s"),
                        duration(node.path("timeouts").path("poll"), "200ms"),
                        duration(node.path("timeouts").path("request"), "15s"),
                        duration(node.path("timeouts").path("read"), "10s"));
        var capture =
                new CaptureDefaults(
                        node.path("capture").path("maxBufferedMessages").asInt(10000),
                        node.path("capture").path("diagnosticsSample").asInt(10));
        var serde =
                new SerdeDefaults(
                        format(node.path("serde").path("key").path("format"), SerdeFormat.STRING),
                        format(node.path("serde").path("value").path("format"), SerdeFormat.JSON));
        var logging =
                new LoggingConfig(
                        payloadLogging(node.path("logging").path("payloads")),
                        node.path("logging").path("maxPayloadChars").asInt(500));
        return new DefaultsConfig(
                timeouts,
                capture,
                asMap(node.path("producer").path("properties")),
                asMap(node.path("consumer").path("properties")),
                asMap(node.path("admin").path("properties")),
                serde,
                logging);
    }

    private static ClusterConfig mapCluster(String name, JsonNode node) {
        return new ClusterConfig(
                name,
                node.path("bootstrapServers").asText(),
                node.path("clientIdPrefix").asText("kafka-testkit"),
                mapSecurity(node.path("security")),
                node.has("schemaRegistry")
                        ? Optional.of(mapSchemaRegistry(node.path("schemaRegistry")))
                        : Optional.empty(),
                mapAdminPolicy(node.path("admin")),
                asMap(node.path("properties")),
                asMap(node.path("producer").path("properties")),
                asMap(node.path("consumer").path("properties")),
                asMap(node.path("admin-client").path("properties")));
    }

    private static SecurityConfig mapSecurity(JsonNode node) {
        var protocol = SecurityProtocol.valueOf(node.path("protocol").asText("PLAINTEXT"));
        var ssl =
                node.has("ssl")
                        ? Optional.of(mapSsl(node.path("ssl")))
                        : Optional.<SslConfig>empty();
        var sasl =
                node.has("sasl")
                        ? Optional.of(mapSasl(node.path("sasl")))
                        : Optional.<SaslConfig>empty();
        return new SecurityConfig(protocol, ssl, sasl);
    }

    private static SslConfig mapSsl(JsonNode node) {
        return new SslConfig(
                node.has("truststore")
                        ? Optional.of(mapStore(node.path("truststore")))
                        : Optional.empty(),
                node.has("keystore")
                        ? Optional.of(mapStore(node.path("keystore")))
                        : Optional.empty(),
                node.has("pem") ? Optional.of(mapPem(node.path("pem"))) : Optional.empty(),
                node.path("endpointIdentification").asText("https"),
                node.has("protocolVersion")
                        ? Optional.of(node.path("protocolVersion").asText())
                        : Optional.empty());
    }

    private static StoreConfig mapStore(JsonNode node) {
        return new StoreConfig(
                node.path("location").asText(),
                secret(node, "password"),
                node.has("keyPassword")
                        ? Optional.of(secret(node, "keyPassword"))
                        : Optional.empty(),
                StoreType.valueOf(node.path("type").asText("JKS")));
    }

    private static PemConfig mapPem(JsonNode node) {
        return new PemConfig(
                node.path("caCertificates").asText(),
                node.path("certificateChain").asText(),
                node.path("privateKey").asText(),
                node.has("privateKeyPassword")
                        ? Optional.of(secret(node, "privateKeyPassword"))
                        : Optional.empty());
    }

    private static SaslConfig mapSasl(JsonNode node) {
        if (node.has("jaasConfig")) {
            return new RawJaasSasl(node.path("mechanism").asText(""), secret(node, "jaasConfig"));
        }
        var mechanism = node.path("mechanism").asText("");
        return switch (mechanism) {
            case "PLAIN" -> new PlainSasl(node.path("username").asText(), secret(node, "password"));
            case "SCRAM-SHA-256" ->
                    new ScramSasl(
                            ScramMechanism.SCRAM_SHA_256,
                            node.path("username").asText(),
                            secret(node, "password"));
            case "SCRAM-SHA-512" ->
                    new ScramSasl(
                            ScramMechanism.SCRAM_SHA_512,
                            node.path("username").asText(),
                            secret(node, "password"));
            case "OAUTHBEARER" -> mapOAuth(node.path("oauth"));
            case "GSSAPI" -> mapGssapi(node.path("kerberos"));
            default ->
                    throw new ConfigurationException(
                            "mecanismo SASL '"
                                    + mechanism
                                    + "' no soportado; usa 'jaasConfig' para mecanismos no modelados");
        };
    }

    private static OAuthBearerSasl mapOAuth(JsonNode node) {
        return new OAuthBearerSasl(
                node.has("tokenEndpointUrl")
                        ? Optional.of(node.path("tokenEndpointUrl").asText())
                        : Optional.empty(),
                node.has("clientId")
                        ? Optional.of(node.path("clientId").asText())
                        : Optional.empty(),
                node.has("clientSecret")
                        ? Optional.of(secret(node, "clientSecret"))
                        : Optional.empty(),
                node.has("scope") ? Optional.of(node.path("scope").asText()) : Optional.empty(),
                asStringMap(node.path("extensions")),
                node.has("callbackHandlerClass")
                        ? Optional.of(node.path("callbackHandlerClass").asText())
                        : Optional.empty(),
                asStringMap(node.path("options")));
    }

    private static GssapiSasl mapGssapi(JsonNode node) {
        return new GssapiSasl(
                node.path("serviceName").asText(),
                node.path("principal").asText(),
                node.has("keytab") ? Optional.of(node.path("keytab").asText()) : Optional.empty(),
                node.has("krb5Conf")
                        ? Optional.of(node.path("krb5Conf").asText())
                        : Optional.empty(),
                node.path("useTicketCache").asBoolean(false));
    }

    private static SchemaRegistryConfig mapSchemaRegistry(JsonNode node) {
        return new SchemaRegistryConfig(
                node.path("url").asText(),
                node.has("auth") ? mapSchemaRegistryAuth(node.path("auth")) : defaultAuth(),
                node.has("ssl") ? Optional.of(mapSsl(node.path("ssl"))) : Optional.empty(),
                node.path("cacheCapacity").asInt(1000),
                asMap(node.path("properties")));
    }

    private static SchemaRegistryAuth mapSchemaRegistryAuth(JsonNode node) {
        return new SchemaRegistryAuth(
                SchemaRegistryAuthType.valueOf(node.path("type").asText("NONE")),
                node.has("username")
                        ? Optional.of(node.path("username").asText())
                        : Optional.empty(),
                node.has("password") ? Optional.of(secret(node, "password")) : Optional.empty(),
                node.has("token") ? Optional.of(secret(node, "token")) : Optional.empty());
    }

    private static SchemaRegistryAuth defaultAuth() {
        return new SchemaRegistryAuth(
                SchemaRegistryAuthType.NONE, Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static AdminPolicy mapAdminPolicy(JsonNode node) {
        var protectedTopics = new ArrayList<String>();
        node.path("protectedTopics").forEach(n -> protectedTopics.add(n.asText()));
        return new AdminPolicy(
                node.path("allowDestructive").asBoolean(false),
                List.copyOf(protectedTopics),
                node.path("temporaryTopicPrefix").asText("kt-tmp-"),
                node.path("allowedTemporaryOnly").asBoolean(false));
    }

    private static TopicConfig mapTopic(String logicalName, JsonNode node, SerdeDefaults defaults) {
        return new TopicConfig(
                logicalName,
                node.path("cluster").asText(),
                node.path("name").asText(logicalName),
                mapSerde(node.path("key"), defaults.key()),
                mapSerde(node.path("value"), defaults.value()),
                asStringMap(node.path("headers").path("defaults")),
                node.has("definition")
                        ? Optional.of(mapTopicDefinition(node.path("definition")))
                        : Optional.empty());
    }

    private static SerdeConfig mapSerde(JsonNode node, SerdeFormat defaultFormat) {
        return new SerdeConfig(
                format(node.path("format"), defaultFormat),
                node.path("charset").asText("UTF-8"),
                node.has("subject") ? Optional.of(node.path("subject").asText()) : Optional.empty(),
                SubjectNameStrategy.valueOf(node.path("subjectNameStrategy").asText("TOPIC")),
                node.has("schemaFile")
                        ? Optional.of(node.path("schemaFile").asText())
                        : Optional.empty(),
                node.has("version") ? Optional.of(node.path("version").asText()) : Optional.empty(),
                node.has("messageType")
                        ? Optional.of(node.path("messageType").asText())
                        : Optional.empty(),
                node.has("specificClass")
                        ? Optional.of(node.path("specificClass").asText())
                        : Optional.empty(),
                asMap(node.path("properties")));
    }

    private static TopicDefinition mapTopicDefinition(JsonNode node) {
        return new TopicDefinition(
                node.path("partitions").asInt(),
                (short) node.path("replicationFactor").asInt(),
                asStringMap(node.path("configs")));
    }

    private static java.time.Duration duration(JsonNode node, String defaultText) {
        var text = node.isMissingNode() ? defaultText : node.asText();
        return DurationParser.tryParse(text)
                .orElseThrow(
                        () ->
                                new ConfigurationException(
                                        "'" + text + "' no es una duración válida"));
    }

    private static SerdeFormat format(JsonNode node, SerdeFormat defaultFormat) {
        return node.isMissingNode() ? defaultFormat : SerdeFormat.valueOf(node.asText());
    }

    private static PayloadLogging payloadLogging(JsonNode node) {
        return node.isMissingNode()
                ? PayloadLogging.TRUNCATED
                : PayloadLogging.valueOf(node.asText());
    }

    private static Secret secret(JsonNode parent, String field) {
        return Secret.of(parent.path(field).asText());
    }

    private static Map<String, Object> asMap(JsonNode node) {
        var result = new LinkedHashMap<String, Object>();
        node.properties().forEach(e -> result.put(e.getKey(), toJavaValue(e.getValue())));
        return Map.copyOf(result);
    }

    private static Map<String, String> asStringMap(JsonNode node) {
        var result = new LinkedHashMap<String, String>();
        node.properties().forEach(e -> result.put(e.getKey(), e.getValue().asText()));
        return Map.copyOf(result);
    }

    private static Object toJavaValue(JsonNode node) {
        if (node.isBoolean()) {
            return node.booleanValue();
        }
        if (node.isIntegralNumber()) {
            return node.longValue();
        }
        if (node.isFloatingPointNumber()) {
            return node.doubleValue();
        }
        if (node.isNull() || node.isMissingNode()) {
            return null;
        }
        if (node.isObject()) {
            var map = new LinkedHashMap<String, Object>();
            node.properties().forEach(e -> map.put(e.getKey(), toJavaValue(e.getValue())));
            return map;
        }
        if (node.isArray()) {
            var list = new ArrayList<Object>();
            node.forEach(n -> list.add(toJavaValue(n)));
            return list;
        }
        return node.asText();
    }
}
