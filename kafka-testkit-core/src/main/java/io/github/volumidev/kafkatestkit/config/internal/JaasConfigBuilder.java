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

import java.util.List;

/** Construye líneas JAAS para {@code sasl.jaas.config}, escapando comillas y backslashes. */
public final class JaasConfigBuilder {

    private JaasConfigBuilder() {}

    /**
     * Una entrada {@code clave=valor} de la línea JAAS; {@code quoted} indica si va entre comillas.
     */
    public record Entry(String key, String value, boolean quoted) {

        /**
         * Crea una entrada con el valor entre comillas (el caso habitual: usuarios, contraseñas,
         * rutas...).
         *
         * @param key clave de la entrada
         * @param value valor, se escapa al construir la línea
         * @return la entrada
         */
        public static Entry quoted(String key, String value) {
            return new Entry(key, value, true);
        }

        /**
         * Crea una entrada con el valor literal, sin comillas (flags booleanos como {@code
         * useKeyTab=true}).
         *
         * @param key clave de la entrada
         * @param value valor literal
         * @return la entrada
         */
        public static Entry raw(String key, String value) {
            return new Entry(key, value, false);
        }
    }

    /**
     * Construye la línea JAAS completa para un módulo con sus entradas, en el orden dado.
     *
     * @param loginModuleClass clase del login module
     * @param entries entradas {@code clave=valor}, en orden
     * @return la línea JAAS, terminada en {@code ;}
     */
    public static String build(String loginModuleClass, List<Entry> entries) {
        var sb = new StringBuilder(loginModuleClass).append(" required");
        for (var entry : entries) {
            sb.append(' ').append(entry.key()).append('=');
            if (entry.quoted()) {
                sb.append('"').append(escape(entry.value())).append('"');
            } else {
                sb.append(entry.value());
            }
        }
        return sb.append(';').toString();
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
