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
package io.github.volumidev.kafkatestkit;

import io.github.volumidev.kafkatestkit.config.KafkaTestKitConfig;
import io.github.volumidev.kafkatestkit.config.KafkaTestKitConfigBuilder;
import java.util.List;

/**
 * Punto de entrada de kafka-testkit.
 *
 * <p>Carga la configuración de forma perezosa: {@code load(...)} solo parsea y valida el YAML, sin
 * abrir ninguna conexión a Kafka.
 */
public final class KafkaTestKit implements AutoCloseable {

    private final KafkaTestKitConfig config;

    private KafkaTestKit(KafkaTestKitConfig config) {
        this.config = config;
    }

    /**
     * Carga la configuración desde la ruta por defecto (ver {@code docs/03} §1), sin entorno.
     *
     * @return el kit con la configuración cargada
     */
    public static KafkaTestKit load() {
        return new KafkaTestKit(builder().build());
    }

    /**
     * Carga la configuración desde un fichero.
     *
     * @param location ruta {@code classpath:}, {@code file:} o de fichero
     * @return el kit con la configuración cargada
     */
    public static KafkaTestKit load(String location) {
        return new KafkaTestKit(builder().config(location).build());
    }

    /**
     * Carga la configuración desde un fichero, fusionando un entorno.
     *
     * @param location ruta {@code classpath:}, {@code file:} o de fichero
     * @param environment entorno a fusionar
     * @return el kit con la configuración cargada
     */
    public static KafkaTestKit load(String location, String environment) {
        return new KafkaTestKit(builder().config(location).environment(environment).build());
    }

    /**
     * Carga la configuración desde varios ficheros (el último sobrescribe a los anteriores),
     * fusionando un entorno.
     *
     * @param locations rutas a combinar, en orden de precedencia creciente
     * @param environment entorno a fusionar
     * @return el kit con la configuración cargada
     */
    public static KafkaTestKit load(List<String> locations, String environment) {
        var builder = builder().environment(environment);
        locations.forEach(builder::config);
        return new KafkaTestKit(builder.build());
    }

    /**
     * Builder para construir el kit con overrides.
     *
     * @return un builder vacío
     */
    public static KafkaTestKitConfigBuilder builder() {
        return new KafkaTestKitConfigBuilder();
    }

    /**
     * La configuración cargada.
     *
     * @return la configuración
     */
    public KafkaTestKitConfig config() {
        return config;
    }

    /**
     * Cierra el kit. En esta fase no hay ninguna conexión que cerrar; las fases siguientes añadirán
     * el cierre de capturas, producers, admins y topics temporales.
     */
    @Override
    public void close() {
        // no-op: F2 no abre conexiones. Ver docs/02 §5 para el cierre ordenado que llega en F3+.
    }
}
