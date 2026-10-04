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

import java.util.Map;
import java.util.Optional;

/**
 * Un topic declarado en el YAML.
 *
 * @param logicalName nombre lógico (la clave en {@code topics}), el que se usa en el código de test
 * @param cluster nombre lógico del cluster al que pertenece
 * @param name nombre físico en Kafka; defecto el nombre lógico
 * @param key serde de la clave
 * @param value serde del valor
 * @param headerDefaults headers añadidos a todos los envíos de este topic
 * @param definition definición física, para Testcontainers y {@code admin.ensureTopic}
 */
public record TopicConfig(
        String logicalName,
        String cluster,
        String name,
        SerdeConfig key,
        SerdeConfig value,
        Map<String, String> headerDefaults,
        Optional<TopicDefinition> definition) {

    /** Copia defensivamente {@code headerDefaults} para mantener el record inmutable. */
    public TopicConfig {
        headerDefaults = Map.copyOf(headerDefaults);
    }
}
