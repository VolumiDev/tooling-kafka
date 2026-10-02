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
 * Valores por defecto globales, aplicables a todos los clusters y topics salvo que se sobrescriban.
 *
 * @param timeouts timeouts por defecto
 * @param capture límites por defecto de captura
 * @param producerProperties propiedades nativas para todos los producers
 * @param consumerProperties propiedades nativas para todos los consumers
 * @param adminProperties propiedades nativas para todos los admin clients
 * @param serde formato por defecto de clave/valor
 * @param logging configuración de diagnóstico de payloads
 */
public record DefaultsConfig(
        TimeoutsConfig timeouts,
        CaptureDefaults capture,
        Map<String, Object> producerProperties,
        Map<String, Object> consumerProperties,
        Map<String, Object> adminProperties,
        SerdeDefaults serde,
        LoggingConfig logging) {

    /** Copia defensivamente los mapas de propiedades para mantener el record inmutable. */
    public DefaultsConfig {
        producerProperties = Map.copyOf(producerProperties);
        consumerProperties = Map.copyOf(consumerProperties);
        adminProperties = Map.copyOf(adminProperties);
    }
}
