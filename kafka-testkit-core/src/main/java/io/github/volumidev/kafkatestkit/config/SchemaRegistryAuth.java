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
 * Autenticación contra Schema Registry.
 *
 * @param type mecanismo de autenticación
 * @param username usuario, para {@link SchemaRegistryAuthType#BASIC}
 * @param password contraseña, para {@link SchemaRegistryAuthType#BASIC}
 * @param token token estático, para {@link SchemaRegistryAuthType#BEARER}
 */
public record SchemaRegistryAuth(
        SchemaRegistryAuthType type,
        Optional<String> username,
        Optional<Secret> password,
        Optional<Secret> token) {}
