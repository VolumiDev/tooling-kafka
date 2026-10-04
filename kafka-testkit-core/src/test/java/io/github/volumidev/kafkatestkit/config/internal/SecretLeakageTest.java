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

import io.github.volumidev.kafkatestkit.config.Secret;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Ningún {@code toString()}, log ni mensaje de excepción debe exponer un secreto. */
class SecretLeakageTest {

    private static final String SECRET_VALUE = "s3cr3t-p4ss";
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
        System.setProperty("KAFKA_PASSWORD", SECRET_VALUE);
        System.setProperty("TS_PASS", SECRET_VALUE);
        System.setProperty("SR_USER", "sr-user");
        System.setProperty("SR_PASSWORD", SECRET_VALUE);
        System.setProperty("LEGACY_TS_PASS", SECRET_VALUE);
        System.setProperty("LEGACY_KS_PASS", SECRET_VALUE);
    }

    @AfterEach
    void clearVariables() {
        GOLDEN_PROPERTIES.forEach(System::clearProperty);
    }

    @Test
    void secret_toString_isAlwaysMasked() {
        assertThat(Secret.of(SECRET_VALUE).toString())
                .isEqualTo("****")
                .doesNotContain(SECRET_VALUE);
    }

    @Test
    void describe_ofGoldenConfig_neverContainsRealSecrets() {
        var config =
                ConfigResolver.resolve(
                        List.of("classpath:config/golden/kafka-testkit-example.yml"),
                        Optional.empty(),
                        Map.of());

        assertThat(config.describe()).doesNotContain(SECRET_VALUE);
    }

    @Test
    void configurationException_message_neverContainsPasswordValue() {
        var resourceLoader = new ResourceLoader();
        var rawTree =
                new YamlConfigLoader(resourceLoader)
                        .loadAndMerge(List.of("classpath:config/golden/kafka-testkit-example.yml"));
        var interpolated =
                Interpolator.interpolate(rawTree, EnvironmentSource.system(), resourceLoader)
                        .resolved();

        var errors = ConfigValidator.validate(interpolated, resourceLoader);
        var describedErrors = errors.stream().map(e -> e.describe()).reduce("", (a, b) -> a + b);

        assertThat(describedErrors).doesNotContain(SECRET_VALUE);
    }
}
