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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.volumidev.kafkatestkit.exception.ConfigurationException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class YamlConfigLoaderTest {

    private final ResourceLoader resourceLoader = new ResourceLoader();
    private final YamlConfigLoader loader = new YamlConfigLoader(resourceLoader);

    @Test
    void loadAndMerge_singleFile_parsesContent() {
        var tree = loader.loadAndMerge(List.of("classpath:config/golden/kafka-topics-common.yml"));
        assertThat(tree.path("topics").path("heartbeat").path("cluster").textValue())
                .isEqualTo("main");
    }

    @Test
    void loadAndMerge_resolvesIncludeBeforeOwnContent() {
        System.setProperty("KAFKA_MAIN_BOOTSTRAP", "localhost:9092");
        System.setProperty("KAFKA_USER", "u");
        System.setProperty("KAFKA_PASSWORD", "p");
        System.setProperty("TS_PASS", "p");
        System.setProperty("SR_USER", "u");
        System.setProperty("SR_PASSWORD", "p");
        System.setProperty("LEGACY_TS_PASS", "p");
        System.setProperty("LEGACY_KS_PASS", "p");
        try {
            var tree =
                    loader.loadAndMerge(
                            List.of("classpath:config/golden/kafka-testkit-example.yml"));
            assertThat(tree.path("topics").path("heartbeat").path("cluster").textValue())
                    .isEqualTo("main");
            assertThat(tree.path("topics").path("orders").path("cluster").textValue())
                    .isEqualTo("main");
        } finally {
            clearGoldenProperties();
        }
    }

    @Test
    void loadAndMerge_missingFile_throwsWithClearMessage() {
        assertThatThrownBy(() -> loader.loadAndMerge(List.of("classpath:does-not-exist.yml")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("does-not-exist.yml");
    }

    @Test
    void loadAndMerge_multipleFiles_lastWins() {
        var tree =
                loader.loadAndMerge(
                        List.of(
                                "classpath:config/golden/kafka-topics-common.yml",
                                "classpath:config/multifile/override.yml"));
        assertThat(tree.path("topics").path("heartbeat").path("name").textValue())
                .isEqualTo("overridden.heartbeat.v1");
    }

    @Test
    void resolveDefaultLocation_systemProperty_takesPrecedence() {
        var env =
                FakeEnvironmentSource.withSystemProperties(
                        Map.of("kafka.testkit.config", "classpath:custom.yml"));
        assertThat(YamlConfigLoader.resolveDefaultLocation(env, resourceLoader))
                .isEqualTo("classpath:custom.yml");
    }

    @Test
    void resolveDefaultLocation_noneFound_throws() {
        var env = FakeEnvironmentSource.withSystemProperties(Map.of());
        assertThatThrownBy(() -> YamlConfigLoader.resolveDefaultLocation(env, resourceLoader))
                .isInstanceOf(ConfigurationException.class);
    }

    @Test
    void resolveEnvironment_explicitArgument_takesPrecedence() {
        var env = FakeEnvironmentSource.withSystemProperties(Map.of("kafka.testkit.env", "pre"));
        assertThat(YamlConfigLoader.resolveEnvironment(Optional.of("local"), env))
                .contains("local");
    }

    @Test
    void resolveEnvironment_karateEnv_isLastFallback() {
        var env = FakeEnvironmentSource.withSystemProperties(Map.of("karate.env", "embedded"));
        assertThat(YamlConfigLoader.resolveEnvironment(Optional.empty(), env)).contains("embedded");
    }

    @Test
    void resolveEnvironment_none_returnsEmpty() {
        var env = FakeEnvironmentSource.withSystemProperties(Map.of());
        assertThat(YamlConfigLoader.resolveEnvironment(Optional.empty(), env)).isEmpty();
    }

    private static void clearGoldenProperties() {
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
}
