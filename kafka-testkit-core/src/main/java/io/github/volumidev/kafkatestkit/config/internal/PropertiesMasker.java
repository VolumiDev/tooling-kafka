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

import io.github.volumidev.kafkatestkit.config.Secret;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Enmascara valores sensibles al mostrar propiedades nativas en logs/{@code describe()}: cualquier
 * {@link Secret}, y cualquier clave que contenga {@code password}, {@code secret}, {@code token} o
 * {@code keyPassword} (p. ej. en mapas de propiedades libres como {@code properties}).
 */
public final class PropertiesMasker {

    private static final Set<String> MARKERS = Set.of("password", "secret", "token", "keypassword");
    private static final String MASK = "****";

    private PropertiesMasker() {}

    /**
     * Indica si una clave debe considerarse sensible por su nombre.
     *
     * @param key clave de la propiedad
     * @return {@code true} si el nombre contiene alguno de los marcadores de secreto
     */
    public static boolean isSecretKey(String key) {
        var lower = key.toLowerCase(Locale.ROOT);
        return MARKERS.stream().anyMatch(lower::contains);
    }

    /**
     * Devuelve una copia del mapa con los valores sensibles enmascarados.
     *
     * @param properties propiedades a enmascarar
     * @return una copia con los secretos sustituidos por {@value #MASK}
     */
    public static Map<String, Object> mask(Map<String, Object> properties) {
        var result = new LinkedHashMap<String, Object>();
        properties.forEach((key, value) -> result.put(key, maskValue(key, value)));
        return Map.copyOf(result);
    }

    private static Object maskValue(String key, Object value) {
        if (value instanceof Secret || isSecretKey(key)) {
            return MASK;
        }
        return value;
    }
}
