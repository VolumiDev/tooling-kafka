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

import java.time.Duration;

/**
 * Timeouts por defecto, aplicables salvo que una operación indique uno explícito.
 *
 * @param await espera por defecto en {@code awaitOne}/{@code awaitCount}/{@code awaitLag}
 * @param poll intervalo de poll de capturas y lecturas
 * @param request timeout de {@code send()}, admin y metadatos
 * @param read tiempo máximo de {@code read()} si no se indica
 */
public record TimeoutsConfig(Duration await, Duration poll, Duration request, Duration read) {}
