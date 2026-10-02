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

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Parsea duraciones del YAML: {@code <número><unidad>} ({@code ms}, {@code s}, {@code m}, {@code
 * h}) o ISO-8601 ({@code PT30S}).
 */
public final class DurationParser {

    private static final Pattern SHORT_FORM = Pattern.compile("(\\d+)(ms|s|m|h)");

    private DurationParser() {}

    /**
     * Intenta parsear una duración, sin lanzar si el texto no es válido.
     *
     * @param text texto a parsear, p. ej. {@code "20s"}, {@code "500ms"}, {@code "PT30S"}
     * @return la duración, o vacío si {@code text} no es una duración válida
     */
    public static Optional<Duration> tryParse(String text) {
        if (text == null) {
            return Optional.empty();
        }
        var trimmed = text.strip();
        var shortForm = SHORT_FORM.matcher(trimmed);
        if (shortForm.matches()) {
            var amount = Long.parseLong(shortForm.group(1));
            return Optional.of(
                    switch (shortForm.group(2)) {
                        case "ms" -> Duration.ofMillis(amount);
                        case "s" -> Duration.ofSeconds(amount);
                        case "m" -> Duration.ofMinutes(amount);
                        case "h" -> Duration.ofHours(amount);
                        default -> throw new IllegalStateException(shortForm.group(2));
                    });
        }
        try {
            return Optional.of(Duration.parse(trimmed));
        } catch (DateTimeParseException e) {
            return Optional.empty();
        }
    }
}
