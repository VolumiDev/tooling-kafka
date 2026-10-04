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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class ResourceLoaderTest {

    private final ResourceLoader resourceLoader = new ResourceLoader();

    @Test
    void exists_classpathResourcePresent_returnsTrue() {
        assertThat(resourceLoader.exists("classpath:certs/truststore.jks")).isTrue();
    }

    @Test
    void exists_classpathResourceAbsent_returnsFalse() {
        assertThat(resourceLoader.exists("classpath:certs/does-not-exist.jks")).isFalse();
    }

    @Test
    void exists_fileAbsent_returnsFalse() {
        assertThat(resourceLoader.exists("/does/not/exist")).isFalse();
    }

    @Test
    void readText_classpathResource_stripsTrailingNewline() {
        assertThat(resourceLoader.readText("classpath:certs/truststore.jks"))
                .isEqualTo("dummy-truststore");
    }

    @Test
    void readText_file_returnsContent() throws IOException {
        var file = Files.createTempFile("kafka-testkit", ".txt");
        Files.writeString(file, "contenido-de-prueba\n");
        try {
            assertThat(resourceLoader.readText(file.toString())).isEqualTo("contenido-de-prueba");
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    void exists_filePrefix_isStripped() {
        var tempDir = Path.of(System.getProperty("java.io.tmpdir"));
        assertThat(resourceLoader.exists("file:" + tempDir)).isTrue();
    }
}
