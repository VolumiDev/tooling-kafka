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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Properties;

/**
 * Versión de kafka-testkit en ejecución.
 *
 * <p>Se lee de un recurso generado en el build, así que coincide con la versión del artefacto
 * publicado. Útil en logs y diagnósticos.
 */
public final class KafkaTestKitVersion {

    static final String UNKNOWN = "unknown";

    private static final String RESOURCE = "version.properties";
    private static final String VERSION =
            load(KafkaTestKitVersion.class.getResourceAsStream(RESOURCE));

    private KafkaTestKitVersion() {}

    /**
     * Devuelve la versión de la librería.
     *
     * @return la versión (p. ej. {@code 0.1.0}) o {@code "unknown"} si no se puede determinar
     */
    public static String current() {
        return VERSION;
    }

    /** Lee la clave {@code version} del recurso; {@value #UNKNOWN} si falta o está sin filtrar. */
    static String load(InputStream in) {
        if (in == null) {
            return UNKNOWN;
        }
        try (in) {
            var props = new Properties();
            props.load(in);
            var version = props.getProperty("version", "").strip();
            return version.isEmpty() || version.startsWith("${") ? UNKNOWN : version;
        } catch (IOException e) {
            throw new UncheckedIOException("No se pudo leer " + RESOURCE, e);
        }
    }
}
