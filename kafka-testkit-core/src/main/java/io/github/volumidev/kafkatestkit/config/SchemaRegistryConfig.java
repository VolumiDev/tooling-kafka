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
 * Configuración de Schema Registry de un cluster.
 *
 * @param url una o varias URLs separadas por coma
 * @param auth autenticación contra el registry
 * @param ssl TLS del registry, si usa una CA propia o requiere certificado de cliente
 * @param cacheCapacity tamaño de la caché de esquemas (defecto {@code 1000})
 * @param properties propiedades nativas del cliente/serializers Confluent
 */
public record SchemaRegistryConfig(
        String url,
        SchemaRegistryAuth auth,
        Optional<SslConfig> ssl,
        int cacheCapacity,
        Map<String, Object> properties) {

    /** Copia defensivamente {@code properties} para mantener el record inmutable. */
    public SchemaRegistryConfig {
        properties = Map.copyOf(properties);
    }
}
