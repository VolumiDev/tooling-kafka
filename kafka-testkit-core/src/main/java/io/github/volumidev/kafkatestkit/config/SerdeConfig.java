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

import java.util.Map;
import java.util.Optional;

/**
 * Serde de una clave o un valor de topic.
 *
 * @param format formato de serialización
 * @param charset codificación de texto, para {@link SerdeFormat#STRING} y {@link SerdeFormat#JSON}
 *     (defecto UTF-8)
 * @param subject subject de Schema Registry, para AVRO/PROTOBUF/JSON_SCHEMA; defecto según {@code
 *     subjectNameStrategy}
 * @param subjectNameStrategy estrategia de nombrado de subject
 * @param schemaFile ruta a un esquema local ({@code .avsc}/{@code .proto}/{@code .json}) para
 *     producir
 * @param version versión del esquema en el registry ({@code latest} por defecto o un número)
 * @param messageType nombre completo del mensaje Protobuf, si el {@code .proto} tiene varios
 * @param specificClass clase generada para obtener objetos tipados en vez de {@code
 *     GenericRecord}/{@code DynamicMessage}
 * @param properties propiedades nativas del serializer/deserializer
 */
public record SerdeConfig(
        SerdeFormat format,
        String charset,
        Optional<String> subject,
        SubjectNameStrategy subjectNameStrategy,
        Optional<String> schemaFile,
        Optional<String> version,
        Optional<String> messageType,
        Optional<String> specificClass,
        Map<String, Object> properties) {

    /** Copia defensivamente {@code properties} para mantener el record inmutable. */
    public SerdeConfig {
        properties = Map.copyOf(properties);
    }
}
