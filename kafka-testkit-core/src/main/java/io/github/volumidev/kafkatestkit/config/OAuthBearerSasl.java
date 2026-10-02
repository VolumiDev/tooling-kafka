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
 * SASL/OAUTHBEARER (OIDC).
 *
 * @param tokenEndpointUrl URL del token endpoint del IdP
 * @param clientId client id OAuth2
 * @param clientSecret client secret OAuth2
 * @param scope scope solicitado
 * @param extensions extensiones JAAS adicionales (p. ej. {@code logicalCluster} en Confluent
 *     Cloud), se añaden como {@code extension_<clave>="<valor>"}
 * @param callbackHandlerClass clase de callback handler propia, para token estático o lógica a
 *     medida; si se indica, sustituye al handler por defecto de Kafka
 * @param options opciones adicionales añadidas a la línea JAAS, usadas junto con {@code
 *     callbackHandlerClass}
 */
public record OAuthBearerSasl(
        Optional<String> tokenEndpointUrl,
        Optional<String> clientId,
        Optional<Secret> clientSecret,
        Optional<String> scope,
        Map<String, String> extensions,
        Optional<String> callbackHandlerClass,
        Map<String, String> options)
        implements SaslConfig {

    /**
     * Copia defensivamente {@code extensions}/{@code options} para mantener el record inmutable.
     */
    public OAuthBearerSasl {
        extensions = Map.copyOf(extensions);
        options = Map.copyOf(options);
    }
}
