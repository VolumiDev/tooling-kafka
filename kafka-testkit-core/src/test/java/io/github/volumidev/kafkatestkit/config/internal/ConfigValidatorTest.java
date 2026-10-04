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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.github.volumidev.kafkatestkit.exception.ConfigError;
import java.util.List;
import org.junit.jupiter.api.Test;

class ConfigValidatorTest {

    private static final YAMLMapper YAML = YAMLMapper.builder().build();
    private final ResourceLoader resourceLoader = new ResourceLoader();

    @Test
    void validate_topicReferencesUnknownCluster_reportsExactMessage() {
        var errors =
                validate(
                        """
                        clusters:
                          main: { bootstrapServers: "localhost:9092" }
                          legacy: { bootstrapServers: "localhost:9093" }
                        topics:
                          orders: { cluster: mian, name: orders }
                        """);

        assertThat(describeAll(errors))
                .contains(
                        "topics.orders.cluster: cluster 'mian' no existe. Clusters definidos: [main, legacy]");
    }

    @Test
    void validate_schemaFormatWithoutSchemaRegistry_reportsError() {
        var errors =
                validate(
                        """
                        clusters:
                          main: { bootstrapServers: "localhost:9092" }
                        topics:
                          orders:
                            cluster: main
                            value: { format: AVRO }
                        """);

        assertThat(describeAll(errors))
                .anyMatch(
                        m ->
                                m.contains("topics.orders.value.format")
                                        && m.contains("schemaRegistry.url"));
    }

    @Test
    void validate_missingTruststoreFile_reportsError() {
        var errors =
                validate(
                        """
                        clusters:
                          main:
                            bootstrapServers: "localhost:9092"
                            security:
                              protocol: SSL
                              ssl:
                                truststore: { location: "classpath:does-not-exist.jks", password: p }
                        """);

        assertThat(describeAll(errors))
                .anyMatch(m -> m.contains("no se encuentra 'classpath:does-not-exist.jks'"));
    }

    @Test
    void validate_scramWithoutCredentials_reportsBothMissingFields() {
        var errors =
                validate(
                        """
                        clusters:
                          main:
                            bootstrapServers: "localhost:9092"
                            security:
                              protocol: SASL_SSL
                              sasl: { mechanism: SCRAM-SHA-512 }
                        """);

        var messages = describeAll(errors);
        assertThat(messages)
                .anyMatch(m -> m.contains("clusters.main.security.sasl.username"))
                .anyMatch(m -> m.contains("clusters.main.security.sasl.password"));
    }

    @Test
    void validate_unknownKey_suggestsClosestMatch() {
        var errors =
                validate(
                        """
                        clusters:
                          main: { bootstrapServers: "localhost:9092" }
                        topics:
                          orders:
                            cluster: main
                            vaule: { format: STRING }
                        """);

        assertThat(describeAll(errors))
                .contains("topics.orders.vaule: clave desconocida. ¿Quisiste decir 'value'?");
    }

    @Test
    void validate_invalidDuration_reportsExactMessage() {
        var errors =
                validate(
                        """
                        defaults:
                          timeouts: { await: "30 segundos" }
                        """);

        assertThat(describeAll(errors))
                .contains(
                        "defaults.timeouts.await: '30 segundos' no es una duración válida (ej: 500ms, 30s, 2m)");
    }

    @Test
    void validate_plaintextWithSsl_isForbidden() {
        var errors =
                validate(
                        """
                        clusters:
                          main:
                            bootstrapServers: "localhost:9092"
                            security:
                              protocol: PLAINTEXT
                              ssl: { truststore: { location: "classpath:certs/truststore.jks", password: p } }
                        """);

        assertThat(describeAll(errors)).anyMatch(m -> m.contains("clusters.main.security.ssl"));
    }

    @Test
    void validate_saslPlaintextWithoutSasl_isRequired() {
        var errors =
                validate(
                        """
                        clusters:
                          main:
                            bootstrapServers: "localhost:9092"
                            security:
                              protocol: SASL_PLAINTEXT
                        """);

        assertThat(describeAll(errors)).anyMatch(m -> m.contains("clusters.main.security.sasl"));
    }

    @Test
    void validate_validTree_reportsNoErrors() {
        var errors =
                validate(
                        """
                        defaults:
                          timeouts: { await: 30s }
                        clusters:
                          main:
                            bootstrapServers: "localhost:9092"
                            security: { protocol: PLAINTEXT }
                        topics:
                          orders: { cluster: main, value: { format: STRING } }
                        """);

        assertThat(errors).isEmpty();
    }

    private List<ConfigError> validate(String yaml) {
        return ConfigValidator.validate(parse(yaml), resourceLoader);
    }

    private static List<String> describeAll(List<ConfigError> errors) {
        return errors.stream().map(ConfigError::describe).toList();
    }

    private static JsonNode parse(String yaml) {
        try {
            return YAML.readTree(yaml);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
