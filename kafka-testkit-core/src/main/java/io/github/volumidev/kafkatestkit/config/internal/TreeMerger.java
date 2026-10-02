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
import java.util.List;

/**
 * Motor único de fusión en profundidad, usado tanto para combinar varios ficheros/{@code include}
 * (el último gana) como para fusionar {@code environments.<entorno>} sobre la base.
 *
 * <p>Reglas: los mapas se fusionan recursivamente clave a clave; listas y escalares se reemplazan;
 * un {@code null} explícito elimina la clave; el bloque {@code security} se reemplaza completo (no
 * se fusiona campo a campo) si el override indica {@code protocol}, para evitar mezclas
 * incoherentes como PLAINTEXT+SASL.
 */
public final class TreeMerger {

    private static final String SECURITY_KEY = "security";
    private static final String PROTOCOL_KEY = "protocol";

    private TreeMerger() {}

    /**
     * Fusiona varios árboles en el orden dado; el último sobrescribe a los anteriores.
     *
     * @param trees árboles a fusionar, en orden de precedencia creciente
     * @return el árbol fusionado
     */
    public static JsonNode mergeAll(List<JsonNode> trees) {
        JsonNode result = null;
        for (var tree : trees) {
            result = result == null ? tree : merge(result, tree);
        }
        return result;
    }

    /**
     * Fusiona {@code override} sobre {@code base}.
     *
     * @param base árbol base
     * @param override árbol que sobrescribe
     * @return el árbol fusionado; ni {@code base} ni {@code override} se modifican
     */
    public static JsonNode merge(JsonNode base, JsonNode override) {
        if (!base.isObject() || !override.isObject()) {
            return override;
        }
        var result = ((ObjectNode) base).deepCopy();
        override.properties()
                .forEach(
                        entry -> {
                            var key = entry.getKey();
                            var overrideValue = entry.getValue();
                            if (overrideValue.isNull()) {
                                result.remove(key);
                                return;
                            }
                            var baseValue = result.get(key);
                            if (baseValue != null
                                    && baseValue.isObject()
                                    && overrideValue.isObject()) {
                                if (SECURITY_KEY.equals(key) && overrideValue.has(PROTOCOL_KEY)) {
                                    result.set(key, overrideValue);
                                } else {
                                    result.set(key, merge(baseValue, overrideValue));
                                }
                            } else {
                                result.set(key, overrideValue);
                            }
                        });
        return result;
    }
}
