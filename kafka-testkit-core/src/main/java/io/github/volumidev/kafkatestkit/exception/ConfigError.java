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
package io.github.volumidev.kafkatestkit.exception;

import java.util.Optional;

/**
 * Un error de configuración localizado en una ruta del YAML (p. ej. {@code topics.orders.cluster}).
 *
 * @param path ruta del YAML donde se detectó el error, con la misma sintaxis que los overrides del
 *     builder (p. ej. {@code clusters.main.bootstrapServers})
 * @param message mensaje descriptivo, listo para mostrar al usuario
 * @param suggestion sugerencia opcional (p. ej. "¿Quisiste decir 'value'?") cuando el error es una
 *     clave desconocida con un candidato cercano
 */
public record ConfigError(String path, String message, Optional<String> suggestion) {

    /**
     * Crea un error sin sugerencia.
     *
     * @param path ruta del YAML donde se detectó el error
     * @param message mensaje descriptivo
     * @return el error creado
     */
    public static ConfigError of(String path, String message) {
        return new ConfigError(path, message, Optional.empty());
    }

    /**
     * Crea un error con una sugerencia de corrección.
     *
     * @param path ruta del YAML donde se detectó el error
     * @param message mensaje descriptivo
     * @param suggestion texto de la sugerencia (sin el prefijo "¿Quisiste decir...?")
     * @return el error creado
     */
    public static ConfigError withSuggestion(String path, String message, String suggestion) {
        return new ConfigError(path, message, Optional.of(suggestion));
    }

    /**
     * Mensaje completo, incluyendo la ruta y, si existe, la sugerencia.
     *
     * @return el mensaje formateado para mostrar al usuario
     */
    public String describe() {
        var base = path + ": " + message;
        return suggestion.map(s -> base + " ¿Quisiste decir '" + s + "'?").orElse(base);
    }
}
