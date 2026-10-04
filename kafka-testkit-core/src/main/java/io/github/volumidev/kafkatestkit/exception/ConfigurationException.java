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

import java.util.List;

/**
 * La configuración no se pudo cargar: contiene todos los errores detectados, no solo el primero.
 */
public class ConfigurationException extends KafkaTestKitException {

    private static final long serialVersionUID = 1L;

    private final transient List<ConfigError> errors;

    /**
     * Crea la excepción a partir de los errores acumulados durante la validación.
     *
     * @param errors errores detectados, no vacía
     */
    public ConfigurationException(List<ConfigError> errors) {
        super(buildMessage(errors));
        this.errors = List.copyOf(errors);
    }

    /**
     * Crea la excepción con un único error puntual (p. ej. entorno inexistente).
     *
     * @param message mensaje descriptivo
     */
    public ConfigurationException(String message) {
        super(message);
        this.errors = List.of(ConfigError.of("", message));
    }

    /**
     * Todos los errores detectados durante la carga de configuración.
     *
     * @return la lista de errores, nunca vacía
     */
    public List<ConfigError> errors() {
        return errors;
    }

    private static String buildMessage(List<ConfigError> errors) {
        return errors.stream().map(ConfigError::describe).reduce((a, b) -> a + "\n" + b).orElse("");
    }
}
