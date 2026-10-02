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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import org.junit.jupiter.api.Test;

class InterpolatorTest {

    private final ResourceLoader resourceLoader = new ResourceLoader();

    @Test
    void interpolate_systemPropertyThenEnvVar_systemPropertyWins() {
        var env =
                new FakeEnvironmentSource(Map.of("VAR", "de-sistema"), Map.of("VAR", "de-entorno"));
        assertThat(resolve("${VAR}", env)).isEqualTo("de-sistema");
    }

    @Test
    void interpolate_missingVariableWithDefault_usesDefault() {
        var env = FakeEnvironmentSource.withSystemProperties(Map.of());
        assertThat(resolve("${VAR:defecto}", env)).isEqualTo("defecto");
    }

    @Test
    void interpolate_missingVariableWithEmptyDefault_usesEmptyString() {
        var env = FakeEnvironmentSource.withSystemProperties(Map.of());
        assertThat(resolve("${VAR:}", env)).isEqualTo("");
    }

    @Test
    void interpolate_missingVariableWithoutDefault_accumulatesError() {
        var env = FakeEnvironmentSource.withSystemProperties(Map.of());
        var root = wrap("${VAR}");
        var result = Interpolator.interpolate(root, env, resourceLoader);
        assertThat(result.errors()).hasSize(1);
        assertThat(result.errors().get(0).describe())
                .contains("la variable VAR no está definida y no tiene valor por defecto");
    }

    @Test
    void interpolate_envPrefix_readsOnlyEnvironmentVariable() {
        var env =
                new FakeEnvironmentSource(Map.of("VAR", "de-sistema"), Map.of("VAR", "de-entorno"));
        assertThat(resolve("${env.VAR}", env)).isEqualTo("de-entorno");
    }

    @Test
    void interpolate_sysPrefix_readsOnlySystemProperty() {
        var env = new FakeEnvironmentSource(Map.of("prop.name", "valor"), Map.of());
        assertThat(resolve("${sys.prop.name}", env)).isEqualTo("valor");
    }

    @Test
    void interpolate_filePrefix_readsFileContent() throws IOException {
        var file = Files.createTempFile("kafka-testkit-interpolator", ".txt");
        Files.writeString(file, "secreto-de-fichero\n");
        try {
            var env = FakeEnvironmentSource.withSystemProperties(Map.of());
            assertThat(resolve("${file:" + file + "}", env)).isEqualTo("secreto-de-fichero");
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    void interpolate_escape_producesLiteralWithoutResolving() {
        var env = FakeEnvironmentSource.withSystemProperties(Map.of());
        assertThat(resolve("$${literal}", env)).isEqualTo("${literal}");
    }

    @Test
    void interpolate_nestedDefault_resolvesInnerVariable() {
        var env =
                FakeEnvironmentSource.withSystemProperties(
                        Map.of("DEFAULT_BOOTSTRAP", "localhost:9092"));
        assertThat(resolve("${KAFKA_BOOTSTRAP:${DEFAULT_BOOTSTRAP:fallback}}", env))
                .isEqualTo("localhost:9092");
    }

    @Test
    void interpolate_nestedDefault_bothMissing_usesInnermostDefault() {
        var env = FakeEnvironmentSource.withSystemProperties(Map.of());
        assertThat(resolve("${KAFKA_BOOTSTRAP:${DEFAULT_BOOTSTRAP:fallback}}", env))
                .isEqualTo("fallback");
    }

    @Test
    void interpolate_textWithoutVariables_isLeftUnchanged() {
        var env = FakeEnvironmentSource.withSystemProperties(Map.of());
        assertThat(resolve("plain text", env)).isEqualTo("plain text");
    }

    private String resolve(String text, EnvironmentSource env) {
        var root = wrap(text);
        var result = Interpolator.interpolate(root, env, resourceLoader);
        return result.resolved().get("v").textValue();
    }

    private static ObjectNode wrap(String text) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("v", text);
        return node;
    }
}
