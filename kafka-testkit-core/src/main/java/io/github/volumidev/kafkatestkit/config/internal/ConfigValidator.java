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
import io.github.volumidev.kafkatestkit.exception.ConfigError;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Valida un árbol de configuración ya interpolado y fusionado por entorno: claves conocidas,
 * referencias cruzadas, combinaciones de seguridad, existencia de ficheros y formato de duraciones.
 * Acumula todos los errores en vez de lanzar en el primero.
 */
public final class ConfigValidator {

    private final ResourceLoader resourceLoader;
    private final List<ConfigError> errors = new ArrayList<>();

    private ConfigValidator(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    /**
     * Valida el árbol completo.
     *
     * @param root árbol ya interpolado y fusionado
     * @param resourceLoader loader usado para comprobar existencia de ficheros
     * @return todos los errores detectados; vacío si la configuración es válida
     */
    public static List<ConfigError> validate(JsonNode root, ResourceLoader resourceLoader) {
        var validator = new ConfigValidator(resourceLoader);
        validator.validateRoot(root);
        return List.copyOf(validator.errors);
    }

    private void validateRoot(JsonNode root) {
        checkKeys(root, "", KnownKeys.ROOT);
        var clusterNames = fieldNames(root.path("clusters"));

        validateDefaults(root.path("defaults"), "defaults");
        root.path("clusters")
                .properties()
                .forEach(e -> validateCluster(e.getValue(), "clusters." + e.getKey()));
        root.path("topics")
                .properties()
                .forEach(
                        e ->
                                validateTopic(
                                        e.getValue(), "topics." + e.getKey(), root, clusterNames));
        root.path("environments")
                .properties()
                .forEach(e -> validateEnvironmentEntry(e.getValue(), "environments." + e.getKey()));
    }

    private void validateEnvironmentEntry(JsonNode env, String path) {
        checkKeys(env, path, KnownKeys.ENVIRONMENT_ENTRY);
        var clusterNames = fieldNames(env.path("clusters"));
        env.path("clusters")
                .properties()
                .forEach(e -> validateCluster(e.getValue(), path + ".clusters." + e.getKey()));
        env.path("topics")
                .properties()
                .forEach(
                        e ->
                                validateTopic(
                                        e.getValue(),
                                        path + ".topics." + e.getKey(),
                                        env,
                                        clusterNames));
    }

    private void validateDefaults(JsonNode defaults, String path) {
        if (defaults.isMissingNode()) {
            return;
        }
        checkKeys(defaults, path, KnownKeys.DEFAULTS);
        validateTimeouts(defaults.path("timeouts"), path + ".timeouts");
        checkKeys(defaults.path("capture"), path + ".capture", KnownKeys.CAPTURE_DEFAULTS);
        checkKeys(
                defaults.path("producer"), path + ".producer", KnownKeys.NATIVE_PROPERTIES_HOLDER);
        checkKeys(
                defaults.path("consumer"), path + ".consumer", KnownKeys.NATIVE_PROPERTIES_HOLDER);
        checkKeys(defaults.path("admin"), path + ".admin", KnownKeys.NATIVE_PROPERTIES_HOLDER);
        validateSerdeDefaults(defaults.path("serde"), path + ".serde");
        checkKeys(defaults.path("logging"), path + ".logging", KnownKeys.LOGGING);
    }

    private void validateSerdeDefaults(JsonNode serde, String path) {
        if (serde.isMissingNode()) {
            return;
        }
        checkKeys(serde, path, KnownKeys.SERDE_DEFAULTS);
        checkKeys(serde.path("key"), path + ".key", KnownKeys.SERDE_DEFAULT_ENTRY);
        checkKeys(serde.path("value"), path + ".value", KnownKeys.SERDE_DEFAULT_ENTRY);
    }

    private void validateTimeouts(JsonNode timeouts, String path) {
        if (timeouts.isMissingNode()) {
            return;
        }
        checkKeys(timeouts, path, KnownKeys.TIMEOUTS);
        timeouts.properties().forEach(e -> validateDuration(e.getValue(), path + "." + e.getKey()));
    }

    private void validateDuration(JsonNode node, String path) {
        if (!node.isTextual()) {
            return;
        }
        var text = node.textValue();
        if (DurationParser.tryParse(text).isEmpty()) {
            errors.add(
                    ConfigError.of(
                            path, "'" + text + "' no es una duración válida (ej: 500ms, 30s, 2m)"));
        }
    }

    private void validateCluster(JsonNode cluster, String path) {
        checkKeys(cluster, path, KnownKeys.CLUSTER);
        checkKeys(cluster.path("producer"), path + ".producer", KnownKeys.NATIVE_PROPERTIES_HOLDER);
        checkKeys(cluster.path("consumer"), path + ".consumer", KnownKeys.NATIVE_PROPERTIES_HOLDER);
        checkKeys(
                cluster.path("admin-client"),
                path + ".admin-client",
                KnownKeys.NATIVE_PROPERTIES_HOLDER);
        validateAdminPolicy(cluster.path("admin"), path + ".admin");
        var security = cluster.path("security");
        validateSecurity(security, path + ".security");
        validateSchemaRegistry(cluster.path("schemaRegistry"), path + ".schemaRegistry");
    }

    private void validateAdminPolicy(JsonNode admin, String path) {
        if (admin.isMissingNode()) {
            return;
        }
        checkKeys(admin, path, KnownKeys.ADMIN_POLICY);
    }

    private void validateSecurity(JsonNode security, String path) {
        if (security.isMissingNode()) {
            return;
        }
        checkKeys(security, path, KnownKeys.SECURITY);
        var protocol = security.path("protocol").asText("PLAINTEXT");
        var hasSsl = security.has("ssl");
        var hasSasl = security.has("sasl");
        switch (protocol) {
            case "PLAINTEXT" -> {
                if (hasSsl) {
                    errors.add(
                            ConfigError.of(path + ".ssl", "no permitido con protocol=PLAINTEXT"));
                }
                if (hasSasl) {
                    errors.add(
                            ConfigError.of(path + ".sasl", "no permitido con protocol=PLAINTEXT"));
                }
            }
            case "SSL" -> {
                if (hasSasl) {
                    errors.add(ConfigError.of(path + ".sasl", "no permitido con protocol=SSL"));
                }
            }
            case "SASL_PLAINTEXT" -> {
                if (hasSsl) {
                    errors.add(
                            ConfigError.of(
                                    path + ".ssl", "no permitido con protocol=SASL_PLAINTEXT"));
                }
                if (!hasSasl) {
                    errors.add(
                            ConfigError.of(
                                    path + ".sasl", "es obligatorio con protocol=SASL_PLAINTEXT"));
                }
            }
            case "SASL_SSL" -> {
                if (!hasSasl) {
                    errors.add(
                            ConfigError.of(path + ".sasl", "es obligatorio con protocol=SASL_SSL"));
                }
            }
            default ->
                    errors.add(
                            ConfigError.of(
                                    path + ".protocol",
                                    "'"
                                            + protocol
                                            + "' no es un protocolo válido (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)"));
        }
        if (hasSsl) {
            validateSsl(security.path("ssl"), path + ".ssl");
        }
        if (hasSasl) {
            validateSasl(security.path("sasl"), path + ".sasl");
        }
    }

    private void validateSsl(JsonNode ssl, String path) {
        checkKeys(ssl, path, KnownKeys.SSL);
        validateStore(ssl.path("truststore"), path + ".truststore");
        validateStore(ssl.path("keystore"), path + ".keystore");
        if (ssl.has("pem")) {
            var pem = ssl.path("pem");
            checkKeys(pem, path + ".pem", KnownKeys.PEM);
            checkFileExists(pem, "caCertificates", path + ".pem.caCertificates");
            checkFileExists(pem, "certificateChain", path + ".pem.certificateChain");
            checkFileExists(pem, "privateKey", path + ".pem.privateKey");
        }
    }

    private void validateStore(JsonNode store, String path) {
        if (store.isMissingNode()) {
            return;
        }
        checkKeys(store, path, KnownKeys.STORE);
        checkFileExists(store, "location", path + ".location");
        if (!store.has("password")) {
            errors.add(ConfigError.of(path + ".password", "es obligatorio"));
        }
    }

    private void checkFileExists(JsonNode node, String field, String path) {
        if (!node.has(field) || !node.path(field).isTextual()) {
            return;
        }
        var location = node.path(field).textValue();
        if (!resourceLoader.exists(location)) {
            errors.add(ConfigError.of(path, "no se encuentra '" + location + "'"));
        }
    }

    private void validateSasl(JsonNode sasl, String path) {
        checkKeys(sasl, path, KnownKeys.SASL);
        if (sasl.has("jaasConfig")) {
            return;
        }
        var mechanism = sasl.path("mechanism").asText("");
        switch (mechanism) {
            case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512" -> {
                if (!sasl.has("username")) {
                    errors.add(
                            ConfigError.of(path + ".username", "es obligatorio para " + mechanism));
                }
                if (!sasl.has("password")) {
                    errors.add(
                            ConfigError.of(path + ".password", "es obligatorio para " + mechanism));
                }
            }
            case "OAUTHBEARER" -> {
                var oauth = sasl.path("oauth");
                checkKeys(oauth, path + ".oauth", KnownKeys.OAUTH);
                if (!oauth.has("callbackHandlerClass")) {
                    if (!oauth.has("clientId")) {
                        errors.add(
                                ConfigError.of(
                                        path + ".oauth.clientId",
                                        "es obligatorio para OAUTHBEARER"));
                    }
                    if (!oauth.has("clientSecret")) {
                        errors.add(
                                ConfigError.of(
                                        path + ".oauth.clientSecret",
                                        "es obligatorio para OAUTHBEARER"));
                    }
                }
            }
            case "GSSAPI" -> {
                var kerberos = sasl.path("kerberos");
                checkKeys(kerberos, path + ".kerberos", KnownKeys.KERBEROS);
                if (!kerberos.has("serviceName")) {
                    errors.add(
                            ConfigError.of(
                                    path + ".kerberos.serviceName", "es obligatorio para GSSAPI"));
                }
                if (!kerberos.has("principal")) {
                    errors.add(
                            ConfigError.of(
                                    path + ".kerberos.principal", "es obligatorio para GSSAPI"));
                }
            }
            case "" -> errors.add(ConfigError.of(path + ".mechanism", "es obligatorio"));
            default -> {
                /* mecanismo no modelado: válido vía properties/jaasConfig */
            }
        }
    }

    private void validateSchemaRegistry(JsonNode sr, String path) {
        if (sr.isMissingNode()) {
            return;
        }
        checkKeys(sr, path, KnownKeys.SCHEMA_REGISTRY);
        if (sr.has("auth")) {
            checkKeys(sr.path("auth"), path + ".auth", KnownKeys.SCHEMA_REGISTRY_AUTH);
        }
        if (sr.has("ssl")) {
            validateSsl(sr.path("ssl"), path + ".ssl");
        }
    }

    private void validateTopic(
            JsonNode topic, String path, JsonNode scope, Set<String> clusterNames) {
        checkKeys(topic, path, KnownKeys.TOPIC);
        var clusterRef = topic.path("cluster").asText(null);
        if (clusterRef != null && !clusterNames.contains(clusterRef)) {
            errors.add(
                    ConfigError.of(
                            path + ".cluster",
                            "cluster '"
                                    + clusterRef
                                    + "' no existe. Clusters definidos: "
                                    + clusterNames));
        }
        checkKeys(topic.path("headers"), path + ".headers", KnownKeys.TOPIC_HEADERS);
        validateTopicSerde(topic.path("key"), path + ".key", clusterRef, scope);
        validateTopicSerde(topic.path("value"), path + ".value", clusterRef, scope);
        var definition = topic.path("definition");
        if (!definition.isMissingNode()) {
            checkKeys(definition, path + ".definition", KnownKeys.TOPIC_DEFINITION);
        }
    }

    private void validateTopicSerde(
            JsonNode serde, String path, String clusterRef, JsonNode scope) {
        if (serde.isMissingNode()) {
            return;
        }
        checkKeys(serde, path, KnownKeys.SERDE_CONFIG);
        var format = serde.path("format").asText("");
        if (isSchemaRegistryFormat(format) && clusterRef != null) {
            var cluster = scope.path("clusters").path(clusterRef);
            var hasUrl = cluster.path("schemaRegistry").has("url");
            if (!hasUrl) {
                errors.add(
                        ConfigError.of(
                                path + ".format",
                                "format="
                                        + format
                                        + " requiere clusters."
                                        + clusterRef
                                        + ".schemaRegistry.url"));
            }
        }
    }

    private static boolean isSchemaRegistryFormat(String format) {
        return "AVRO".equals(format) || "PROTOBUF".equals(format) || "JSON_SCHEMA".equals(format);
    }

    private void checkKeys(JsonNode node, String path, Set<String> allowed) {
        if (node.isMissingNode() || !node.isObject()) {
            return;
        }
        node.properties()
                .forEach(
                        entry -> {
                            var key = entry.getKey();
                            if (!allowed.contains(key)) {
                                var childPath = path.isEmpty() ? key : path + "." + key;
                                Suggestion.closest(key, allowed)
                                        .ifPresentOrElse(
                                                suggestion ->
                                                        errors.add(
                                                                ConfigError.withSuggestion(
                                                                        childPath,
                                                                        "clave desconocida.",
                                                                        suggestion)),
                                                () ->
                                                        errors.add(
                                                                ConfigError.of(
                                                                        childPath,
                                                                        "clave desconocida.")));
                            }
                        });
    }

    private static Set<String> fieldNames(JsonNode node) {
        var names = new java.util.LinkedHashSet<String>();
        node.fieldNames().forEachRemaining(names::add);
        return java.util.Collections.unmodifiableSet(names);
    }
}
