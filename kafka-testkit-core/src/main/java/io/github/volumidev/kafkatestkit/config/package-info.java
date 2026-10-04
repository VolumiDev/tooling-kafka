/**
 * Modelo de configuración de kafka-testkit: records inmutables que representan el YAML ya resuelto
 * (interpolado, fusionado por entorno y validado), más la fachada {@link
 * io.github.volumidev.kafkatestkit.config.KafkaTestKitConfigBuilder} para cargarlo con overrides.
 *
 * <p>Los paquetes {@code internal} contienen la implementación (carga, interpolación, merge,
 * validación, mapeo de seguridad) y no forman parte de la API pública.
 */
package io.github.volumidev.kafkatestkit.config;
