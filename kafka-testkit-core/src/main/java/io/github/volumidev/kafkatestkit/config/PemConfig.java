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
 * mTLS en formato PEM, alternativa a {@link StoreConfig} sin necesidad de empaquetar un keystore.
 *
 * @param caCertificates ruta a la CA (truststore)
 * @param certificateChain ruta a la cadena de certificado de cliente
 * @param privateKey ruta a la clave privada del cliente
 * @param privateKeyPassword contraseña de la clave privada, si está cifrada
 */
public record PemConfig(
        String caCertificates,
        String certificateChain,
        String privateKey,
        Optional<Secret> privateKeyPassword) {}
