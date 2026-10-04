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

import java.util.Set;

/** Claves válidas en cada posición del árbol de configuración, usadas para detectar errores. */
final class KnownKeys {

    static final Set<String> ROOT =
            Set.of("include", "defaults", "clusters", "topics", "environments", "testcontainers");

    static final Set<String> ENVIRONMENT_ENTRY = Set.of("clusters", "topics", "testcontainers");

    static final Set<String> DEFAULTS =
            Set.of("timeouts", "capture", "producer", "consumer", "admin", "serde", "logging");
    static final Set<String> TIMEOUTS = Set.of("await", "poll", "request", "read");
    static final Set<String> CAPTURE_DEFAULTS = Set.of("maxBufferedMessages", "diagnosticsSample");
    static final Set<String> NATIVE_PROPERTIES_HOLDER = Set.of("properties");
    static final Set<String> SERDE_DEFAULTS = Set.of("key", "value");
    static final Set<String> SERDE_DEFAULT_ENTRY = Set.of("format");
    static final Set<String> LOGGING = Set.of("payloads", "maxPayloadChars");

    static final Set<String> CLUSTER =
            Set.of(
                    "bootstrapServers",
                    "clientIdPrefix",
                    "security",
                    "schemaRegistry",
                    "admin",
                    "properties",
                    "producer",
                    "consumer",
                    "admin-client");

    static final Set<String> SECURITY = Set.of("protocol", "ssl", "sasl");
    static final Set<String> SSL =
            Set.of("truststore", "keystore", "pem", "endpointIdentification", "protocolVersion");
    static final Set<String> STORE = Set.of("location", "password", "keyPassword", "type");
    static final Set<String> PEM =
            Set.of("caCertificates", "certificateChain", "privateKey", "privateKeyPassword");
    static final Set<String> SASL =
            Set.of("mechanism", "username", "password", "oauth", "kerberos", "jaasConfig");
    static final Set<String> OAUTH =
            Set.of(
                    "tokenEndpointUrl",
                    "clientId",
                    "clientSecret",
                    "scope",
                    "extensions",
                    "callbackHandlerClass",
                    "options");
    static final Set<String> KERBEROS =
            Set.of("serviceName", "principal", "keytab", "krb5Conf", "useTicketCache");

    static final Set<String> SCHEMA_REGISTRY =
            Set.of("url", "auth", "ssl", "cacheCapacity", "properties");
    static final Set<String> SCHEMA_REGISTRY_AUTH = Set.of("type", "username", "password", "token");

    static final Set<String> ADMIN_POLICY =
            Set.of(
                    "allowDestructive",
                    "protectedTopics",
                    "temporaryTopicPrefix",
                    "allowedTemporaryOnly");

    static final Set<String> TOPIC =
            Set.of("cluster", "name", "key", "value", "headers", "definition");
    static final Set<String> TOPIC_HEADERS = Set.of("defaults");
    static final Set<String> SERDE_CONFIG =
            Set.of(
                    "format",
                    "charset",
                    "subject",
                    "subjectNameStrategy",
                    "schemaFile",
                    "version",
                    "messageType",
                    "specificClass",
                    "properties");
    static final Set<String> TOPIC_DEFINITION =
            Set.of("partitions", "replicationFactor", "configs");

    private KnownKeys() {}
}
