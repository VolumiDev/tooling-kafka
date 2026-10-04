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
package io.github.volumidev.kafkatestkit.config;

import io.github.volumidev.kafkatestkit.config.internal.ConfigResolver;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Construye una {@link KafkaTestKitConfig} a partir de uno o varios ficheros YAML, un entorno
 * opcional y overrides puntuales.
 *
 * <p>Los overrides se aplican después del merge de entorno y antes de la validación, con la misma
 * sintaxis de rutas que los mensajes de error (p. ej. {@code clusters.main.bootstrapServers}).
 */
public final class KafkaTestKitConfigBuilder {

    private final List<String> locations = new ArrayList<>();
    private Optional<String> environment = Optional.empty();
    private final Map<String, String> overrides = new LinkedHashMap<>();

    /** Crea un builder vacío, sin ficheros ni entorno. */
    public KafkaTestKitConfigBuilder() {}

    /**
     * Añade un fichero de configuración. Se puede llamar varias veces; el último sobrescribe a los
     * anteriores.
     *
     * @param location ruta {@code classpath:}, {@code file:} o de fichero
     * @return este builder
     */
    public KafkaTestKitConfigBuilder config(String location) {
        locations.add(location);
        return this;
    }

    /**
     * Selecciona el entorno a fusionar sobre la configuración base.
     *
     * @param environment nombre del entorno ({@code environments.<entorno>})
     * @return este builder
     */
    public KafkaTestKitConfigBuilder environment(String environment) {
        this.environment = Optional.of(environment);
        return this;
    }

    /**
     * Sobrescribe un valor tras el merge de entorno y antes de validar.
     *
     * @param path ruta del valor, con la misma sintaxis que los mensajes de error
     * @param value valor a aplicar
     * @return este builder
     */
    public KafkaTestKitConfigBuilder override(String path, String value) {
        overrides.put(path, value);
        return this;
    }

    /**
     * Carga y valida la configuración con los ficheros, el entorno y los overrides indicados.
     *
     * @return la configuración resuelta
     */
    public KafkaTestKitConfig build() {
        return ConfigResolver.resolve(List.copyOf(locations), environment, Map.copyOf(overrides));
    }
}
