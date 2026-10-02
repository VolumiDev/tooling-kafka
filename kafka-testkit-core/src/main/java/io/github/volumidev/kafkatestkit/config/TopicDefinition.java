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

/**
 * Definición física de un topic, usada por Testcontainers y por {@code admin.ensureTopic}.
 *
 * @param partitions número de particiones
 * @param replicationFactor factor de replicación
 * @param configs configuración nativa del topic (p. ej. {@code retention.ms})
 */
public record TopicDefinition(
        int partitions, short replicationFactor, Map<String, String> configs) {

    /** Copia defensivamente {@code configs} para mantener el record inmutable. */
    public TopicDefinition {
        configs = Map.copyOf(configs);
    }
}
