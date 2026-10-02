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

import java.util.Optional;

/**
 * Configuración TLS de un cluster o de Schema Registry.
 *
 * @param truststore truststore de servidor; si falta en {@code protocol: SSL}, se usa la CA del JDK
 * @param keystore keystore de cliente, para mTLS
 * @param pem alternativa en formato PEM a {@code truststore}/{@code keystore}
 * @param endpointIdentification algoritmo de verificación del hostname ({@code https} por defecto);
 *     cadena vacía lo desactiva (no recomendado)
 * @param protocolVersion versión de TLS (p. ej. {@code TLSv1.3})
 */
public record SslConfig(
        Optional<StoreConfig> truststore,
        Optional<StoreConfig> keystore,
        Optional<PemConfig> pem,
        String endpointIdentification,
        Optional<String> protocolVersion) {}
