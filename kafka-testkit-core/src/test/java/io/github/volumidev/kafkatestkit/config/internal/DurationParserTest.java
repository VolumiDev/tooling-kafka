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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class DurationParserTest {

    @ParameterizedTest
    @CsvSource({"20s,PT20S", "500ms,PT0.5S", "2m,PT2M", "1h,PT1H", "PT30S,PT30S"})
    void tryParse_validText_returnsDuration(String text, String expectedIso) {
        assertThat(DurationParser.tryParse(text)).contains(Duration.parse(expectedIso));
    }

    @Test
    void tryParse_invalidText_returnsEmpty() {
        assertThat(DurationParser.tryParse("30 segundos")).isEmpty();
    }

    @Test
    void tryParse_null_returnsEmpty() {
        assertThat(DurationParser.tryParse(null)).isEmpty();
    }
}
