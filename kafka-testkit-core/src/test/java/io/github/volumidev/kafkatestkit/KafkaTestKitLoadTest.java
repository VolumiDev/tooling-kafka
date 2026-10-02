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
package io.github.volumidev.kafkatestkit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.volumidev.kafkatestkit.exception.ConfigurationException;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaTestKitLoadTest {

    private static final String LOCATION = "classpath:config/golden/kafka-testkit-example.yml";
    private static final List<String> GOLDEN_PROPERTIES =
            List.of(
                    "KAFKA_MAIN_BOOTSTRAP",
                    "KAFKA_USER",
                    "KAFKA_PASSWORD",
                    "TS_PASS",
                    "SR_USER",
                    "SR_PASSWORD",
                    "LEGACY_TS_PASS",
                    "LEGACY_KS_PASS");

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
        GOLDEN_PROPERTIES.forEach(System::clearProperty);
    }

    @Test
    void load_withLocationAndEnvironment_buildsConfig() {
        try (var kit = KafkaTestKit.load(LOCATION, "local")) {
            assertThat(kit.config().environment()).contains("local");
            assertThat(kit.config().cluster("main").bootstrapServers()).isEqualTo("localhost:9092");
        }
    }

    @Test
    void load_withMultipleLocations_mergesThem() {
        try (var kit = KafkaTestKit.load(List.of(LOCATION), "local")) {
            assertThat(kit.config().topic("heartbeat")).isNotNull();
        }
    }

    @Test
    void builder_withOverride_appliesIt() {
        var config =
                KafkaTestKit.builder()
                        .config(LOCATION)
                        .environment("local")
                        .override("clusters.main.bootstrapServers", "override:9092")
                        .build();

        assertThat(config.cluster("main").bootstrapServers()).isEqualTo("override:9092");
    }

    @Test
    void config_unknownCluster_throwsWithClearMessage() {
        try (var kit = KafkaTestKit.load(LOCATION, "local")) {
            assertThatThrownBy(() -> kit.config().cluster("missing"))
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining("missing")
                    .hasMessageContaining("main");
        }
    }

    @Test
    void close_isIdempotent() {
        var kit = KafkaTestKit.load(LOCATION, "local");
        kit.close();
        kit.close();
    }
}
