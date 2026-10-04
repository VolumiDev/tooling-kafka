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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class KafkaTestKitVersionTest {

    @Test
    void current_isFilteredFromBuild() {
        assertThat(KafkaTestKitVersion.current())
                .isNotBlank()
                .isNotEqualTo(KafkaTestKitVersion.UNKNOWN)
                .doesNotContain("${");
    }

    @Test
    void load_missingResource_returnsUnknown() {
        assertThat(KafkaTestKitVersion.load(null)).isEqualTo(KafkaTestKitVersion.UNKNOWN);
    }

    @Test
    void load_unfilteredPlaceholder_returnsUnknown() {
        assertThat(KafkaTestKitVersion.load(stream("version=${project.version}")))
                .isEqualTo(KafkaTestKitVersion.UNKNOWN);
    }

    @Test
    void load_validVersion_returnsIt() {
        assertThat(KafkaTestKitVersion.load(stream("version= 1.2.3 "))).isEqualTo("1.2.3");
    }

    private static ByteArrayInputStream stream(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.ISO_8859_1));
    }
}
