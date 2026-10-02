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
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.github.volumidev.kafkatestkit.exception.ConfigurationException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Localiza y parsea el YAML de configuración: resolución de ruta por defecto, {@code include}
 * recursivo y combinación de varios ficheros (el último sobrescribe a los anteriores).
 */
public final class YamlConfigLoader {

    private static final YAMLMapper YAML_MAPPER = YAMLMapper.builder().build();
    private static final String CONFIG_LOCATION_PROPERTY = "kafka.testkit.config";
    private static final String CONFIG_LOCATION_ENV = "KAFKA_TESTKIT_CONFIG";
    private static final String ENV_PROPERTY = "kafka.testkit.env";
    private static final String ENV_VARIABLE = "KAFKA_TESTKIT_ENV";
    private static final String KARATE_ENV_PROPERTY = "karate.env";

    private final ResourceLoader resourceLoader;

    /**
     * Crea el loader.
     *
     * @param resourceLoader usado para comprobar existencia y leer el contenido de los ficheros
     */
    public YamlConfigLoader(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    /**
     * Carga y combina varios ficheros, resolviendo {@code include} en cada uno.
     *
     * @param locations rutas a cargar, en orden de precedencia creciente
     * @return el árbol combinado
     */
    public JsonNode loadAndMerge(List<String> locations) {
        var trees =
                locations.stream()
                        .map(location -> loadWithIncludes(location, new LinkedHashSet<>()))
                        .toList();
        return TreeMerger.mergeAll(trees);
    }

    private JsonNode loadWithIncludes(String location, Set<String> visiting) {
        if (!visiting.add(location)) {
            throw new ConfigurationException("ciclo de 'include' detectado en '" + location + "'");
        }
        var parsed = parse(location);
        var includes = parsed.path("include");
        if (includes.isArray() && !includes.isEmpty()) {
            var trees = new ArrayList<JsonNode>();
            includes.forEach(n -> trees.add(loadWithIncludes(n.asText(), visiting)));
            trees.add(parsed);
            visiting.remove(location);
            return TreeMerger.mergeAll(trees);
        }
        visiting.remove(location);
        return parsed;
    }

    private JsonNode parse(String location) {
        if (!resourceLoader.exists(location)) {
            throw new ConfigurationException(
                    "no se encuentra el fichero de configuración '" + location + "'");
        }
        try {
            return YAML_MAPPER.readTree(resourceLoader.readText(location));
        } catch (Exception e) {
            throw new ConfigurationException(
                    "no se pudo parsear '" + location + "': " + e.getMessage());
        }
    }

    /**
     * Resuelve la ruta por defecto cuando no se indica ninguna explícitamente.
     *
     * @param env fuente de propiedades de sistema y variables de entorno
     * @param resourceLoader usado para comprobar los nombres por defecto en el classpath
     * @return la ruta resuelta
     * @throws ConfigurationException si no se puede resolver ninguna ruta
     */
    public static String resolveDefaultLocation(
            EnvironmentSource env, ResourceLoader resourceLoader) {
        return env.systemProperty(CONFIG_LOCATION_PROPERTY)
                .or(() -> env.environmentVariable(CONFIG_LOCATION_ENV))
                .or(() -> existingClasspath(resourceLoader, "classpath:kafka-testkit.yml"))
                .or(() -> existingClasspath(resourceLoader, "classpath:kafka-testkit.yaml"))
                .orElseThrow(
                        () ->
                                new ConfigurationException(
                                        "no se indicó ningún fichero de configuración y no se encuentra "
                                                + "'kafka-testkit.yml'/'kafka-testkit.yaml' en el classpath"));
    }

    private static Optional<String> existingClasspath(
            ResourceLoader resourceLoader, String location) {
        return resourceLoader.exists(location) ? Optional.of(location) : Optional.empty();
    }

    /**
     * Resuelve el entorno a fusionar, por orden de precedencia.
     *
     * @param explicit entorno indicado explícitamente al builder/{@code load}, si alguno
     * @param env fuente de propiedades de sistema y variables de entorno
     * @return el entorno a fusionar, o vacío si no se seleccionó ninguno
     */
    public static Optional<String> resolveEnvironment(
            Optional<String> explicit, EnvironmentSource env) {
        if (explicit.isPresent()) {
            return explicit;
        }
        return env.systemProperty(ENV_PROPERTY)
                .or(() -> env.environmentVariable(ENV_VARIABLE))
                .or(() -> env.systemProperty(KARATE_ENV_PROPERTY));
    }
}
