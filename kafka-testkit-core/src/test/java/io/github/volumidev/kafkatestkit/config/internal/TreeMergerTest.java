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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import org.junit.jupiter.api.Test;

class TreeMergerTest {

    @Test
    void merge_recursesIntoNestedMaps() {
        var base = objectOf("a", objectOf("x", text("1"), "y", text("2")));
        var override = objectOf("a", objectOf("y", text("20")));

        var result = TreeMerger.merge(base, override);

        assertThat(result.path("a").path("x").textValue()).isEqualTo("1");
        assertThat(result.path("a").path("y").textValue()).isEqualTo("20");
    }

    @Test
    void merge_listsAndScalars_areReplacedNotMerged() {
        var base = objectOf("list", array("a", "b"), "scalar", text("1"));
        var override = objectOf("list", array("c"), "scalar", text("2"));

        var result = TreeMerger.merge(base, override);

        assertThat(result.path("list")).hasSize(1);
        assertThat(result.path("list").get(0).textValue()).isEqualTo("c");
        assertThat(result.path("scalar").textValue()).isEqualTo("2");
    }

    @Test
    void merge_explicitNull_removesKey() {
        var base = objectOf("a", text("1"), "b", text("2"));
        var override = JsonNodeFactory.instance.objectNode();
        override.putNull("a");

        var result = TreeMerger.merge(base, override);

        assertThat(result.has("a")).isFalse();
        assertThat(result.path("b").textValue()).isEqualTo("2");
    }

    @Test
    void merge_securityWithoutProtocolChange_mergesRecursively() {
        var base =
                objectOf(
                        "security",
                        objectOf(
                                "protocol",
                                text("SASL_SSL"),
                                "sasl",
                                objectOf("mechanism", text("PLAIN"))));
        var override = objectOf("security", objectOf("sasl", objectOf("username", text("u"))));

        var result = TreeMerger.merge(base, override);

        assertThat(result.path("security").path("protocol").textValue()).isEqualTo("SASL_SSL");
        assertThat(result.path("security").path("sasl").path("mechanism").textValue())
                .isEqualTo("PLAIN");
        assertThat(result.path("security").path("sasl").path("username").textValue())
                .isEqualTo("u");
    }

    @Test
    void merge_securityWithProtocolChange_replacesEntireBlock() {
        var base =
                objectOf(
                        "security",
                        objectOf(
                                "protocol",
                                text("SASL_SSL"),
                                "sasl",
                                objectOf("mechanism", text("PLAIN"))));
        var override = objectOf("security", objectOf("protocol", text("PLAINTEXT")));

        var result = TreeMerger.merge(base, override);

        assertThat(result.path("security").path("protocol").textValue()).isEqualTo("PLAINTEXT");
        assertThat(result.path("security").has("sasl")).isFalse();
    }

    @Test
    void mergeAll_lastFileWins() {
        var first = objectOf("a", text("1"), "b", text("2"));
        var second = objectOf("a", text("10"));

        var result = TreeMerger.mergeAll(List.of(first, second));

        assertThat(result.path("a").textValue()).isEqualTo("10");
        assertThat(result.path("b").textValue()).isEqualTo("2");
    }

    private static ObjectNode objectOf(Object... keyValues) {
        var node = JsonNodeFactory.instance.objectNode();
        for (int i = 0; i < keyValues.length; i += 2) {
            node.set((String) keyValues[i], (JsonNode) keyValues[i + 1]);
        }
        return node;
    }

    private static JsonNode text(String value) {
        return JsonNodeFactory.instance.textNode(value);
    }

    private static ArrayNode array(String... values) {
        var node = JsonNodeFactory.instance.arrayNode();
        for (var value : values) {
            node.add(value);
        }
        return node;
    }
}
