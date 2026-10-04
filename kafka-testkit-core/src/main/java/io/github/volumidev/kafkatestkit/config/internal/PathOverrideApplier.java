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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.List;
import java.util.Map;

/**
 * Aplica overrides puntuales del builder sobre el árbol de configuración, con la misma sintaxis de
 * rutas que los mensajes de error (p. ej. {@code clusters.main.bootstrapServers}).
 *
 * <p>Se aplican después del merge de entorno y antes de la validación.
 */
public final class PathOverrideApplier {

    private PathOverrideApplier() {}

    /**
     * Aplica todos los overrides sobre el árbol, creando las claves intermedias que falten.
     *
     * @param root árbol base
     * @param overrides rutas y valores a aplicar
     * @return el árbol con los overrides aplicados; {@code root} no se modifica
     */
    public static JsonNode apply(JsonNode root, Map<String, String> overrides) {
        var result = ((ObjectNode) root).deepCopy();
        overrides.forEach((path, value) -> setAtPath(result, splitPath(path), value));
        return result;
    }

    private static List<String> splitPath(String path) {
        return List.of(path.split("\\."));
    }

    private static void setAtPath(ObjectNode root, List<String> segments, String value) {
        var current = root;
        for (int i = 0; i < segments.size() - 1; i++) {
            var segment = segments.get(i);
            var next = current.get(segment);
            if (next == null || !next.isObject()) {
                next = current.putObject(segment);
            } else {
                var copy = ((ObjectNode) next).deepCopy();
                current.set(segment, copy);
                next = copy;
            }
            current = (ObjectNode) next;
        }
        current.set(segments.get(segments.size() - 1), new TextNode(value));
    }
}
