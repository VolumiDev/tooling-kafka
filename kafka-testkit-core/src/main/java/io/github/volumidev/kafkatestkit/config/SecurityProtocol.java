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

/** Protocolo de seguridad de un cluster, igual que {@code security.protocol} de Kafka. */
public enum SecurityProtocol {
    /** Sin cifrar ni autenticar. Solo local / Testcontainers. */
    PLAINTEXT,
    /** TLS de servidor, opcionalmente con certificado de cliente (mTLS). */
    SSL,
    /** SASL sin cifrar. */
    SASL_PLAINTEXT,
    /** SASL sobre TLS. Caso más común en cloud. */
    SASL_SSL
}
