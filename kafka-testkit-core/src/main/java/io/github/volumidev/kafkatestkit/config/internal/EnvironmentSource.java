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

import java.util.Optional;

/**
 * Abstrae el acceso a propiedades de sistema y variables de entorno, para poder inyectar valores en
 * tests sin tocar el proceso real.
 */
public interface EnvironmentSource {

    /**
     * Lee una propiedad de sistema.
     *
     * @param name nombre de la propiedad
     * @return el valor, si está definida
     */
    Optional<String> systemProperty(String name);

    /**
     * Lee una variable de entorno.
     *
     * @param name nombre de la variable
     * @return el valor, si está definida
     */
    Optional<String> environmentVariable(String name);

    /**
     * Fuente que lee del proceso real ({@code System.getProperty}/{@code System.getenv}).
     *
     * @return la fuente del sistema
     */
    static EnvironmentSource system() {
        return new EnvironmentSource() {
            @Override
            public Optional<String> systemProperty(String name) {
                return Optional.ofNullable(System.getProperty(name));
            }

            @Override
            public Optional<String> environmentVariable(String name) {
                return Optional.ofNullable(System.getenv(name));
            }
        };
    }
}
