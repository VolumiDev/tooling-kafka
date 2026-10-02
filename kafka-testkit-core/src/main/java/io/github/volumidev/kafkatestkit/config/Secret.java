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

import java.util.Objects;

/**
 * Envoltorio de un valor sensible (contraseña, token, JAAS literal...).
 *
 * <p>{@link #toString()} nunca revela el valor; solo {@link #reveal()} lo hace, y debe llamarse
 * exclusivamente al construir las propiedades nativas del cliente Kafka, nunca para logging.
 */
public final class Secret {

    private final String value;

    private Secret(String value) {
        this.value = Objects.requireNonNull(value, "value");
    }

    /**
     * Envuelve un valor como secreto.
     *
     * @param value valor en claro
     * @return el secreto creado
     */
    public static Secret of(String value) {
        return new Secret(value);
    }

    /**
     * Devuelve el valor en claro. Único punto de acceso al secreto real.
     *
     * @return el valor envuelto
     */
    public String reveal() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Secret other && value.equals(other.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return "****";
    }
}
