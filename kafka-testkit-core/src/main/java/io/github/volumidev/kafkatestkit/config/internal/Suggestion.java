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

import java.util.Collection;
import java.util.Optional;

/** Sugerencia de corrección ("¿Quisiste decir...?") para una clave desconocida del YAML. */
public final class Suggestion {

    private Suggestion() {}

    /**
     * Busca, entre los candidatos, el más cercano a {@code unknown} por distancia de Levenshtein.
     *
     * @param unknown clave desconocida tal como aparece en el YAML
     * @param candidates claves válidas en esa posición del árbol
     * @return el candidato más cercano, si hay uno dentro del umbral y estrictamente mejor que el
     *     resto; vacío en otro caso
     */
    public static Optional<String> closest(String unknown, Collection<String> candidates) {
        String best = null;
        int bestDistance = Integer.MAX_VALUE;
        boolean tie = false;
        for (var candidate : candidates) {
            var distance = levenshtein(unknown, candidate);
            if (distance < bestDistance) {
                best = candidate;
                bestDistance = distance;
                tie = false;
            } else if (distance == bestDistance) {
                tie = true;
            }
        }
        if (best == null || tie) {
            return Optional.empty();
        }
        var threshold = Math.max(2, unknown.length() / 3);
        return bestDistance <= threshold ? Optional.of(best) : Optional.empty();
    }

    /**
     * Distancia de edición clásica (inserción/borrado/sustitución, coste 1).
     *
     * @param a primera cadena
     * @param b segunda cadena
     * @return el número mínimo de ediciones para transformar {@code a} en {@code b}
     */
    static int levenshtein(String a, String b) {
        var dp = new int[a.length() + 1][b.length() + 1];
        for (int i = 0; i <= a.length(); i++) {
            dp[i][0] = i;
        }
        for (int j = 0; j <= b.length(); j++) {
            dp[0][j] = j;
        }
        for (int i = 1; i <= a.length(); i++) {
            for (int j = 1; j <= b.length(); j++) {
                if (a.charAt(i - 1) == b.charAt(j - 1)) {
                    dp[i][j] = dp[i - 1][j - 1];
                } else {
                    dp[i][j] = 1 + Math.min(dp[i - 1][j - 1], Math.min(dp[i - 1][j], dp[i][j - 1]));
                }
            }
        }
        return dp[a.length()][b.length()];
    }
}
