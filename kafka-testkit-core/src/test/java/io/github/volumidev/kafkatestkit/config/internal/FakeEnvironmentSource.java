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

import java.util.Map;
import java.util.Optional;

/** {@link EnvironmentSource} de prueba, con valores fijados en memoria. */
final class FakeEnvironmentSource implements EnvironmentSource {

    private final Map<String, String> systemProperties;
    private final Map<String, String> environmentVariables;

    FakeEnvironmentSource(
            Map<String, String> systemProperties, Map<String, String> environmentVariables) {
        this.systemProperties = systemProperties;
        this.environmentVariables = environmentVariables;
    }

    static FakeEnvironmentSource withSystemProperties(Map<String, String> properties) {
        return new FakeEnvironmentSource(properties, Map.of());
    }

    @Override
    public Optional<String> systemProperty(String name) {
        return Optional.ofNullable(systemProperties.get(name));
    }

    @Override
    public Optional<String> environmentVariable(String name) {
        return Optional.ofNullable(environmentVariables.get(name));
    }
}
