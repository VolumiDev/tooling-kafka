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
 * Un truststore o keystore de SSL.
 *
 * @param location ruta {@code classpath:} o de fichero
 * @param password contraseña del store
 * @param keyPassword contraseña de la clave privada, solo en un keystore con una clave distinta de
 *     la del store
 * @param type formato del store
 */
public record StoreConfig(
        String location, Secret password, Optional<Secret> keyPassword, StoreType type) {}
