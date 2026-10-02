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

import io.github.volumidev.kafkatestkit.config.SecurityProtocol;
import io.github.volumidev.kafkatestkit.config.SerdeFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * El YAML completo de docs/03 §10 debe cargar en los tres entornos posibles: base (sin entorno),
 * {@code local} y {@code pre}. Es el criterio de aceptación explícito de F2.
 */
class ConfigResolverGoldenTest {

    private static final String LOCATION = "classpath:config/golden/kafka-testkit-example.yml";

    @BeforeEach
    void setUpVariables() {
        System.setProperty("KAFKA_MAIN_BOOTSTRAP", "kafka-main.acme.com:9093");
        System.setProperty("KAFKA_USER", "qa-user");
        System.setProperty("KAFKA_PASSWORD", "qa-pass");
        System.setProperty("TS_PASS", "ts-pass");
        System.setProperty("SR_USER", "sr-user");
        System.setProperty("SR_PASSWORD", "sr-pass");
        System.setProperty("LEGACY_TS_PASS", "legacy-ts-pass");
        System.setProperty("LEGACY_KS_PASS", "legacy-ks-pass");
    }

    @AfterEach
    void clearVariables() {
        for (var key :
                List.of(
                        "KAFKA_MAIN_BOOTSTRAP",
                        "KAFKA_USER",
                        "KAFKA_PASSWORD",
                        "TS_PASS",
                        "SR_USER",
                        "SR_PASSWORD",
                        "LEGACY_TS_PASS",
                        "LEGACY_KS_PASS")) {
            System.clearProperty(key);
        }
    }

    @Test
    void resolve_withoutEnvironment_loadsBaseConfiguration() {
        var config = ConfigResolver.resolve(List.of(LOCATION), Optional.empty(), Map.of());

        assertThat(config.environment()).isEmpty();
        assertThat(config.cluster("main").bootstrapServers()).isEqualTo("kafka-main.acme.com:9093");
        assertThat(config.cluster("main").security().protocol())
                .isEqualTo(SecurityProtocol.SASL_SSL);
        assertThat(config.cluster("legacy").security().protocol()).isEqualTo(SecurityProtocol.SSL);
        assertThat(config.topic("orders").value().format()).isEqualTo(SerdeFormat.AVRO);
        assertThat(config.topic("customers").key().format()).isEqualTo(SerdeFormat.STRING);
        assertThat(config.topic("heartbeat").cluster()).isEqualTo("main");
    }

    @Test
    void resolve_localEnvironment_overridesToPlaintext() {
        var config = ConfigResolver.resolve(List.of(LOCATION), Optional.of("local"), Map.of());

        assertThat(config.environment()).contains("local");
        assertThat(config.cluster("main").bootstrapServers()).isEqualTo("localhost:9092");
        assertThat(config.cluster("main").security().protocol())
                .isEqualTo(SecurityProtocol.PLAINTEXT);
        assertThat(config.cluster("main").security().sasl()).isEmpty();
        assertThat(config.cluster("main").admin().allowDestructive()).isTrue();
        assertThat(config.cluster("legacy").security().protocol())
                .isEqualTo(SecurityProtocol.PLAINTEXT);
    }

    @Test
    void resolve_preEnvironment_onlyOverridesMainBootstrap() {
        var config = ConfigResolver.resolve(List.of(LOCATION), Optional.of("pre"), Map.of());

        assertThat(config.environment()).contains("pre");
        assertThat(config.cluster("main").bootstrapServers()).isEqualTo("kafka-pre.acme.com:9093");
        assertThat(config.cluster("main").security().protocol())
                .isEqualTo(SecurityProtocol.SASL_SSL);
        assertThat(config.cluster("legacy").security().protocol()).isEqualTo(SecurityProtocol.SSL);
    }

    @Test
    void resolve_unknownEnvironment_throwsWithDefinedEnvironments() {
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () ->
                                ConfigResolver.resolve(
                                        List.of(LOCATION), Optional.of("staging"), Map.of()))
                .isInstanceOf(
                        io.github.volumidev.kafkatestkit.exception.ConfigurationException.class)
                .hasMessageContaining("staging")
                .hasMessageContaining("local")
                .hasMessageContaining("pre");
    }

    @Test
    void resolve_withOverride_appliesAfterEnvironmentMerge() {
        var config =
                ConfigResolver.resolve(
                        List.of(LOCATION),
                        Optional.of("local"),
                        Map.of("clusters.main.bootstrapServers", "override:9092"));

        assertThat(config.cluster("main").bootstrapServers()).isEqualTo("override:9092");
    }
}
