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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.github.volumidev.kafkatestkit.exception.ConfigError;
import java.util.ArrayList;
import java.util.List;

/**
 * Resuelve {@code ${...}} en los valores textuales de un árbol de configuración, antes del merge de
 * entornos.
 *
 * <p>Sintaxis soportada: {@code ${VAR}}, {@code ${VAR:defecto}}, {@code ${VAR:}}, {@code
 * ${env.VAR}}, {@code ${sys.prop}}, {@code ${file:/ruta}}, con anidación en el valor por defecto, y
 * el escape {@code $${literal}}.
 */
public final class Interpolator {

    private final EnvironmentSource env;
    private final ResourceLoader resourceLoader;
    private final List<ConfigError> errors = new ArrayList<>();

    private Interpolator(EnvironmentSource env, ResourceLoader resourceLoader) {
        this.env = env;
        this.resourceLoader = resourceLoader;
    }

    /**
     * Interpola todos los valores textuales del árbol.
     *
     * @param root árbol a interpolar
     * @param env fuente de propiedades de sistema y variables de entorno
     * @param resourceLoader loader usado para {@code ${file:...}}
     * @return el árbol interpolado y los errores acumulados
     */
    public static InterpolationResult interpolate(
            JsonNode root, EnvironmentSource env, ResourceLoader resourceLoader) {
        var interpolator = new Interpolator(env, resourceLoader);
        var resolved = interpolator.walk(root, "");
        return new InterpolationResult(resolved, List.copyOf(interpolator.errors));
    }

    private JsonNode walk(JsonNode node, String path) {
        if (node.isObject()) {
            var result = ((ObjectNode) node).objectNode();
            node.properties()
                    .forEach(
                            entry ->
                                    result.set(
                                            entry.getKey(),
                                            walk(
                                                    entry.getValue(),
                                                    childPath(path, entry.getKey()))));
            return result;
        }
        if (node.isArray()) {
            var result = ((ArrayNode) node).arrayNode();
            for (int i = 0; i < node.size(); i++) {
                result.add(walk(node.get(i), path + "[" + i + "]"));
            }
            return result;
        }
        if (node.isTextual()) {
            return new TextNode(interpolateString(node.textValue(), path));
        }
        return node;
    }

    private static String childPath(String parent, String key) {
        return parent.isEmpty() ? key : parent + "." + key;
    }

    private String interpolateString(String input, String path) {
        var out = new StringBuilder();
        int i = 0;
        while (i < input.length()) {
            if (startsWith(input, i, "$${")) {
                int close = findMatchingBrace(input, i + 2);
                out.append('$').append('{').append(input, i + 3, close).append('}');
                i = close + 1;
            } else if (startsWith(input, i, "${")) {
                int close = findMatchingBrace(input, i + 1);
                var expr = input.substring(i + 2, close);
                out.append(resolveExpression(expr, path));
                i = close + 1;
            } else {
                out.append(input.charAt(i));
                i++;
            }
        }
        return out.toString();
    }

    private static boolean startsWith(String input, int index, String prefix) {
        return input.regionMatches(index, prefix, 0, prefix.length());
    }

    /** Busca la llave de cierre que corresponde a la de apertura en {@code openBraceIndex}. */
    private static int findMatchingBrace(String input, int openBraceIndex) {
        int depth = 1;
        int i = openBraceIndex + 1;
        while (i < input.length()) {
            if (startsWith(input, i, "${")) {
                depth++;
                i += 2;
            } else if (input.charAt(i) == '}') {
                depth--;
                if (depth == 0) {
                    return i;
                }
                i++;
            } else {
                i++;
            }
        }
        throw new IllegalArgumentException("'${' sin cerrar en: " + input);
    }

    private String resolveExpression(String expr, String path) {
        if (expr.startsWith("env.")) {
            var name = expr.substring("env.".length());
            return env.environmentVariable(name)
                    .orElseGet(
                            () ->
                                    missing(
                                            path,
                                            "la variable de entorno "
                                                    + name
                                                    + " no está definida"));
        }
        if (expr.startsWith("sys.")) {
            var name = expr.substring("sys.".length());
            return env.systemProperty(name)
                    .orElseGet(
                            () ->
                                    missing(
                                            path,
                                            "la propiedad de sistema "
                                                    + name
                                                    + " no está definida"));
        }
        if (expr.startsWith("file:")) {
            return resourceLoader.readText(expr.substring("file:".length()));
        }
        return resolveVariableWithDefault(expr, path);
    }

    private String resolveVariableWithDefault(String expr, String path) {
        var colon = topLevelColon(expr);
        var name = colon < 0 ? expr : expr.substring(0, colon);
        var value = env.systemProperty(name).or(() -> env.environmentVariable(name));
        if (value.isPresent()) {
            return value.get();
        }
        if (colon < 0) {
            return missing(
                    path, "la variable " + name + " no está definida y no tiene valor por defecto");
        }
        return interpolateString(expr.substring(colon + 1), path);
    }

    /** Primer {@code ':'} que no está dentro de un {@code ${...}} anidado. */
    private static int topLevelColon(String expr) {
        int depth = 0;
        for (int i = 0; i < expr.length(); i++) {
            if (startsWith(expr, i, "${")) {
                depth++;
            } else if (expr.charAt(i) == '}') {
                depth--;
            } else if (expr.charAt(i) == ':' && depth == 0) {
                return i;
            }
        }
        return -1;
    }

    private String missing(String path, String message) {
        errors.add(ConfigError.of(path, message));
        return "";
    }
}
