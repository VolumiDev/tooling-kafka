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
package io.github.volumidev.kafkatestkit.config.internal;

import io.github.volumidev.kafkatestkit.config.KafkaTestKitConfig;

/** Construye el resumen legible de {@link KafkaTestKitConfig#describe()}, sin exponer secretos. */
public final class ConfigDescriber {

    private ConfigDescriber() {}

    /**
     * Construye el resumen.
     *
     * @param config configuración a describir
     * @return el resumen, listo para mostrar en logs
     */
    public static String describe(KafkaTestKitConfig config) {
        var sb = new StringBuilder();
        sb.append("entorno: ").append(config.environment().orElse("(ninguno)")).append('\n');
        sb.append("clusters:\n");
        config.clusters()
                .forEach(
                        (name, cluster) ->
                                sb.append("  - ")
                                        .append(name)
                                        .append(": ")
                                        .append(cluster.bootstrapServers())
                                        .append(" [")
                                        .append(cluster.security().protocol())
                                        .append("]\n"));
        sb.append("topics:\n");
        config.topics()
                .forEach(
                        (name, topic) ->
                                sb.append("  - ")
                                        .append(name)
                                        .append(" -> ")
                                        .append(topic.name())
                                        .append(" (cluster=")
                                        .append(topic.cluster())
                                        .append(", key=")
                                        .append(topic.key().format())
                                        .append(", value=")
                                        .append(topic.value().format())
                                        .append(")\n"));
        return sb.toString();
    }
}
