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

import java.util.List;

/**
 * Política de protección de operaciones destructivas de administración sobre un cluster.
 *
 * <p>{@code ^__.*} y {@code ^_schemas$} están siempre protegidos, aunque no se indiquen aquí.
 *
 * @param allowDestructive si {@code false} (defecto), prohíbe delete/purge/reset/deleteGroup
 * @param protectedTopics expresiones regulares de topics que nunca se tocan, aunque {@code
 *     allowDestructive} sea {@code true}
 * @param temporaryTopicPrefix prefijo de {@code createTemporaryTopic}
 * @param allowedTemporaryOnly si {@code true}, solo se puede borrar/purgar lo creado por el kit
 */
public record AdminPolicy(
        boolean allowDestructive,
        List<String> protectedTopics,
        String temporaryTopicPrefix,
        boolean allowedTemporaryOnly) {

    /** Copia defensivamente {@code protectedTopics} para mantener el record inmutable. */
    public AdminPolicy {
        protectedTopics = List.copyOf(protectedTopics);
    }
}
