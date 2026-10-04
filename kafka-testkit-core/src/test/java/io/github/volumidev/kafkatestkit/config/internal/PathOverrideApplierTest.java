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
import java.util.Map;
import org.junit.jupiter.api.Test;

class PathOverrideApplierTest {

    @Test
    void apply_existingNestedKey_overwritesValue() {
        var root = JsonNodeFactory.instance.objectNode();
        root.putObject("clusters").putObject("main").put("bootstrapServers", "old:9092");

        var result =
                PathOverrideApplier.apply(
                        root, Map.of("clusters.main.bootstrapServers", "new:9092"));

        assertThat(result.path("clusters").path("main").path("bootstrapServers").textValue())
                .isEqualTo("new:9092");
    }

    @Test
    void apply_missingIntermediateKeys_createsThem() {
        var root = JsonNodeFactory.instance.objectNode();

        var result =
                PathOverrideApplier.apply(
                        root, Map.of("clusters.main.bootstrapServers", "host:9092"));

        assertThat(result.path("clusters").path("main").path("bootstrapServers").textValue())
                .isEqualTo("host:9092");
    }

    @Test
    void apply_doesNotMutateOriginalTree() {
        var root = JsonNodeFactory.instance.objectNode();
        root.putObject("clusters").putObject("main").put("bootstrapServers", "old:9092");

        PathOverrideApplier.apply(root, Map.of("clusters.main.bootstrapServers", "new:9092"));

        assertThat(root.path("clusters").path("main").path("bootstrapServers").textValue())
                .isEqualTo("old:9092");
    }
}
