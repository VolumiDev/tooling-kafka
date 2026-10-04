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
import io.github.volumidev.kafkatestkit.config.KafkaTestKitConfig;
import io.github.volumidev.kafkatestkit.exception.ConfigurationException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Orquesta la carga completa de configuración: localizar y parsear ficheros, interpolar variables,
 * fusionar el entorno seleccionado, aplicar overrides y validar, antes de construir los records
 * tipados.
 */
public final class ConfigResolver {

    private ConfigResolver() {}

    /**
     * Resuelve la configuración completa.
     *
     * @param locations ficheros a cargar, en orden de precedencia creciente; si está vacía, se usa
     *     la resolución por defecto ({@code -Dkafka.testkit.config}, {@code KAFKA_TESTKIT_CONFIG},
     *     {@code classpath:kafka-testkit.yml/.yaml})
     * @param environmentArg entorno indicado explícitamente, si alguno
     * @param overrides overrides a aplicar tras el merge de entorno
     * @return la configuración validada y tipada
     */
    public static KafkaTestKitConfig resolve(
            List<String> locations,
            Optional<String> environmentArg,
            Map<String, String> overrides) {
        var env = EnvironmentSource.system();
        var resourceLoader = new ResourceLoader();

        var actualLocations =
                locations.isEmpty()
                        ? List.of(YamlConfigLoader.resolveDefaultLocation(env, resourceLoader))
                        : locations;
        var rawTree = new YamlConfigLoader(resourceLoader).loadAndMerge(actualLocations);

        var interpolated = Interpolator.interpolate(rawTree, env, resourceLoader);
        var errors = new ArrayList<>(interpolated.errors());

        var environment = YamlConfigLoader.resolveEnvironment(environmentArg, env);
        var merged = mergeEnvironment(interpolated.resolved(), environment);

        if (!overrides.isEmpty()) {
            merged = PathOverrideApplier.apply(merged, overrides);
        }

        errors.addAll(ConfigValidator.validate(merged, resourceLoader));
        if (!errors.isEmpty()) {
            throw new ConfigurationException(errors);
        }

        return ConfigTreeToRecordsMapper.map(merged, environment);
    }

    private static JsonNode mergeEnvironment(JsonNode root, Optional<String> environment) {
        if (environment.isEmpty()) {
            return root;
        }
        var name = environment.get();
        var block = root.path("environments").path(name);
        if (block.isMissingNode()) {
            var defined = new ArrayList<String>();
            root.path("environments").fieldNames().forEachRemaining(defined::add);
            throw new ConfigurationException(
                    "el entorno '" + name + "' no existe. Entornos definidos: " + defined);
        }
        return TreeMerger.merge(root, block);
    }
}
