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
 * Un cluster Kafka declarado en el YAML.
 *
 * @param name nombre lógico (la clave en {@code clusters})
 * @param bootstrapServers lista separada por comas de {@code host:puerto}
 * @param clientIdPrefix prefijo de {@code client.id} (defecto {@code kafka-testkit})
 * @param security seguridad del cluster (defecto {@code PLAINTEXT})
 * @param schemaRegistry Schema Registry del cluster, si algún topic lo necesita
 * @param admin política de protección de operaciones destructivas
 * @param properties propiedades nativas comunes a producer/consumer/admin de este cluster
 * @param producerProperties propiedades nativas solo del producer
 * @param consumerProperties propiedades nativas solo del consumer
 * @param adminProperties propiedades nativas solo del admin client
 */
public record ClusterConfig(
        String name,
        String bootstrapServers,
        String clientIdPrefix,
        SecurityConfig security,
        Optional<SchemaRegistryConfig> schemaRegistry,
        AdminPolicy admin,
        Map<String, Object> properties,
        Map<String, Object> producerProperties,
        Map<String, Object> consumerProperties,
        Map<String, Object> adminProperties) {

    /** Copia defensivamente los mapas de propiedades para mantener el record inmutable. */
    public ClusterConfig {
        properties = Map.copyOf(properties);
        producerProperties = Map.copyOf(producerProperties);
        consumerProperties = Map.copyOf(consumerProperties);
        adminProperties = Map.copyOf(adminProperties);
    }
}
