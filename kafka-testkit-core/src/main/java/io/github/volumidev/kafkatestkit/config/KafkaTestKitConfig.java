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

import io.github.volumidev.kafkatestkit.exception.ConfigurationException;
import java.util.Map;
import java.util.Optional;

/**
 * Configuración completa y ya resuelta (interpolada, fusionada por entorno y validada) de
 * kafka-testkit.
 *
 * @param environment entorno seleccionado al cargar, si alguno
 * @param defaults valores por defecto globales
 * @param clusters clusters declarados, por nombre lógico
 * @param topics topics declarados, por nombre lógico
 */
public record KafkaTestKitConfig(
        Optional<String> environment,
        DefaultsConfig defaults,
        Map<String, ClusterConfig> clusters,
        Map<String, TopicConfig> topics) {

    /** Copia defensivamente los mapas de clusters/topics para mantener el record inmutable. */
    public KafkaTestKitConfig {
        clusters = Map.copyOf(clusters);
        topics = Map.copyOf(topics);
    }

    /**
     * Busca un cluster por su nombre lógico.
     *
     * @param name nombre lógico del cluster
     * @return la configuración del cluster
     * @throws ConfigurationException si no existe ningún cluster con ese nombre
     */
    public ClusterConfig cluster(String name) {
        var cluster = clusters.get(name);
        if (cluster == null) {
            throw new ConfigurationException(
                    "cluster '" + name + "' no existe. Clusters definidos: " + clusters.keySet());
        }
        return cluster;
    }

    /**
     * Busca un topic por su nombre lógico.
     *
     * @param name nombre lógico del topic
     * @return la configuración del topic
     * @throws ConfigurationException si no existe ningún topic con ese nombre
     */
    public TopicConfig topic(String name) {
        var topic = topics.get(name);
        if (topic == null) {
            throw new ConfigurationException(
                    "topic '" + name + "' no existe. Topics definidos: " + topics.keySet());
        }
        return topic;
    }

    /**
     * Resumen legible de la configuración cargada, con los secretos enmascarados.
     *
     * @return el resumen
     */
    public String describe() {
        return io.github.volumidev.kafkatestkit.config.internal.ConfigDescriber.describe(this);
    }
}
