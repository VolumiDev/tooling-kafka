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
 * SASL/GSSAPI (Kerberos).
 *
 * @param serviceName nombre del servicio Kafka en el KDC
 * @param principal principal del cliente
 * @param keytab ruta al keytab
 * @param krb5Conf ruta a {@code krb5.conf}; es una propiedad global de la JVM, así que clusters
 *     distintos no pueden indicar valores distintos
 * @param useTicketCache si {@code true}, usa el ticket cache del usuario ({@code kinit}) en vez del
 *     keytab
 */
public record GssapiSasl(
        String serviceName,
        String principal,
        Optional<String> keytab,
        Optional<String> krb5Conf,
        boolean useTicketCache)
        implements SaslConfig {}
