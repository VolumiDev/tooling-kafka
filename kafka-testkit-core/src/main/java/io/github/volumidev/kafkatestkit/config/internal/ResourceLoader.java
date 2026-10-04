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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Resuelve rutas {@code classpath:}, {@code file:} o sin prefijo (fichero) usadas en el YAML:
 * truststores, keystores, {@code schemaFile}, {@code ${file:...}}.
 *
 * <p>En F2 solo se comprueba existencia y se lee contenido de texto; la materialización de recursos
 * de classpath a fichero temporal (necesaria para pasarlos a Kafka) se añade cuando un cliente real
 * los necesite (F3+), para que cargar la configuración no tenga efectos de filesystem.
 */
public final class ResourceLoader {

    private static final String CLASSPATH_PREFIX = "classpath:";
    private static final String FILE_PREFIX = "file:";

    private final ClassLoader classLoader;

    /** Crea un loader que usa el class loader de este hilo. */
    public ResourceLoader() {
        this(Thread.currentThread().getContextClassLoader());
    }

    /**
     * Crea un loader con un class loader explícito, para tests.
     *
     * @param classLoader class loader a usar para rutas {@code classpath:}
     */
    public ResourceLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    /**
     * Comprueba si el recurso existe.
     *
     * @param location ruta {@code classpath:}, {@code file:} o de fichero
     * @return {@code true} si el recurso existe
     */
    public boolean exists(String location) {
        if (location.startsWith(CLASSPATH_PREFIX)) {
            return classLoader.getResource(stripPrefix(location, CLASSPATH_PREFIX)) != null;
        }
        return Files.exists(toPath(location));
    }

    /**
     * Lee el contenido de texto del recurso, sin el salto de línea final.
     *
     * @param location ruta {@code classpath:}, {@code file:} o de fichero
     * @return el contenido, sin salto final
     */
    public String readText(String location) {
        try {
            String content;
            if (location.startsWith(CLASSPATH_PREFIX)) {
                var resource = stripPrefix(location, CLASSPATH_PREFIX);
                try (var in = classLoader.getResourceAsStream(resource)) {
                    if (in == null) {
                        throw new UncheckedIOException(
                                new IOException("No se encuentra el recurso '" + location + "'"));
                    }
                    content =
                            new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                }
            } else {
                content = Files.readString(toPath(location));
            }
            return content.endsWith("\n") ? content.substring(0, content.length() - 1) : content;
        } catch (IOException e) {
            throw new UncheckedIOException("No se pudo leer '" + location + "'", e);
        }
    }

    private static Path toPath(String location) {
        return Path.of(stripPrefix(location, FILE_PREFIX));
    }

    private static String stripPrefix(String location, String prefix) {
        return location.startsWith(prefix) ? location.substring(prefix.length()) : location;
    }
}
