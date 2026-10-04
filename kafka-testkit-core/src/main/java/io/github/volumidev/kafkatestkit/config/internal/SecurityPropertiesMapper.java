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

import io.github.volumidev.kafkatestkit.config.GssapiSasl;
import io.github.volumidev.kafkatestkit.config.OAuthBearerSasl;
import io.github.volumidev.kafkatestkit.config.PemConfig;
import io.github.volumidev.kafkatestkit.config.PlainSasl;
import io.github.volumidev.kafkatestkit.config.RawJaasSasl;
import io.github.volumidev.kafkatestkit.config.SaslConfig;
import io.github.volumidev.kafkatestkit.config.SchemaRegistryAuth;
import io.github.volumidev.kafkatestkit.config.ScramMechanism;
import io.github.volumidev.kafkatestkit.config.ScramSasl;
import io.github.volumidev.kafkatestkit.config.Secret;
import io.github.volumidev.kafkatestkit.config.SecurityConfig;
import io.github.volumidev.kafkatestkit.config.SslConfig;
import io.github.volumidev.kafkatestkit.config.StoreConfig;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Traduce {@link SecurityConfig} y la autenticación de Schema Registry a propiedades nativas de
 * Kafka/Confluent, listas para pasar a un cliente real.
 *
 * <p>No tiene efectos de filesystem salvo leer el contenido de certificados PEM (necesario porque
 * Kafka los recibe como texto, no como ruta); no copia truststores/keystores a ficheros temporales
 * ni toca propiedades globales de la JVM (p. ej. {@code krb5Conf}) — eso ocurre al construir un
 * cliente real.
 */
public final class SecurityPropertiesMapper {

    private SecurityPropertiesMapper() {}

    /**
     * Traduce la seguridad de un cluster a propiedades nativas de Kafka.
     *
     * @param security configuración de seguridad
     * @param resourceLoader usado para leer certificados PEM
     * @return las propiedades, con los secretos ya revelados (listas para el cliente real, nunca
     *     para logging)
     */
    public static Map<String, Object> toKafkaProperties(
            SecurityConfig security, ResourceLoader resourceLoader) {
        var props = new LinkedHashMap<String, Object>();
        props.put("security.protocol", security.protocol().name());
        security.ssl().ifPresent(ssl -> props.putAll(mapSsl(ssl, resourceLoader, "ssl.")));
        security.sasl().ifPresent(sasl -> props.putAll(mapSasl(sasl)));
        return Map.copyOf(props);
    }

    /**
     * Traduce la autenticación de Schema Registry a propiedades de los serializers Confluent.
     *
     * @param auth autenticación configurada
     * @param ssl TLS de Schema Registry, si lo usa
     * @param resourceLoader usado para leer certificados PEM
     * @return las propiedades, con los secretos ya revelados
     */
    public static Map<String, Object> toSchemaRegistryProperties(
            SchemaRegistryAuth auth, Optional<SslConfig> ssl, ResourceLoader resourceLoader) {
        var props = new LinkedHashMap<String, Object>();
        switch (auth.type()) {
            case BASIC -> {
                props.put("basic.auth.credentials.source", "USER_INFO");
                props.put(
                        "basic.auth.user.info",
                        auth.username().orElse("")
                                + ":"
                                + auth.password().map(Secret::reveal).orElse(""));
            }
            case USER_INFO_FROM_SASL -> props.put("basic.auth.credentials.source", "SASL_INHERIT");
            case BEARER -> {
                props.put("bearer.auth.credentials.source", "STATIC_TOKEN");
                auth.token().ifPresent(token -> props.put("bearer.auth.token", token.reveal()));
            }
            case NONE -> {
                /* sin autenticación */
            }
            default ->
                    throw new IllegalStateException(
                            "SchemaRegistryAuthType no soportado: " + auth.type());
        }
        ssl.ifPresent(s -> props.putAll(mapSsl(s, resourceLoader, "schema.registry.ssl.")));
        return Map.copyOf(props);
    }

    private static Map<String, Object> mapSsl(
            SslConfig ssl, ResourceLoader resourceLoader, String prefix) {
        var props = new LinkedHashMap<String, Object>();
        ssl.truststore().ifPresent(store -> props.putAll(mapStore(store, prefix, "truststore")));
        ssl.keystore().ifPresent(store -> props.putAll(mapStore(store, prefix, "keystore")));
        ssl.pem().ifPresent(pem -> props.putAll(mapPem(pem, resourceLoader, prefix)));
        props.put(prefix + "endpoint.identification.algorithm", ssl.endpointIdentification());
        ssl.protocolVersion().ifPresent(version -> props.put(prefix + "protocol", version));
        return props;
    }

    private static Map<String, Object> mapStore(StoreConfig store, String prefix, String kind) {
        var props = new LinkedHashMap<String, Object>();
        props.put(prefix + kind + ".location", store.location());
        props.put(prefix + kind + ".password", store.password().reveal());
        if ("keystore".equals(kind)) {
            store.keyPassword()
                    .ifPresent(
                            keyPassword ->
                                    props.put(prefix + "key.password", keyPassword.reveal()));
        }
        props.put(prefix + kind + ".type", store.type().name());
        return props;
    }

    private static Map<String, Object> mapPem(
            PemConfig pem, ResourceLoader resourceLoader, String prefix) {
        var props = new LinkedHashMap<String, Object>();
        props.put(prefix + "truststore.type", "PEM");
        props.put(
                prefix + "truststore.certificates", resourceLoader.readText(pem.caCertificates()));
        props.put(prefix + "keystore.type", "PEM");
        props.put(
                prefix + "keystore.certificate.chain",
                resourceLoader.readText(pem.certificateChain()));
        props.put(prefix + "keystore.key", resourceLoader.readText(pem.privateKey()));
        pem.privateKeyPassword()
                .ifPresent(password -> props.put(prefix + "key.password", password.reveal()));
        return props;
    }

    private static Map<String, Object> mapSasl(SaslConfig sasl) {
        return switch (sasl) {
            case PlainSasl p ->
                    withJaas(
                            "PLAIN",
                            "org.apache.kafka.common.security.plain.PlainLoginModule",
                            List.of(
                                    JaasConfigBuilder.Entry.quoted("username", p.username()),
                                    JaasConfigBuilder.Entry.quoted(
                                            "password", p.password().reveal())));
            case ScramSasl s ->
                    withJaas(
                            s.mechanism() == ScramMechanism.SCRAM_SHA_256
                                    ? "SCRAM-SHA-256"
                                    : "SCRAM-SHA-512",
                            "org.apache.kafka.common.security.scram.ScramLoginModule",
                            List.of(
                                    JaasConfigBuilder.Entry.quoted("username", s.username()),
                                    JaasConfigBuilder.Entry.quoted(
                                            "password", s.password().reveal())));
            case OAuthBearerSasl o -> mapOAuth(o);
            case GssapiSasl g -> mapGssapi(g);
            case RawJaasSasl r -> {
                var props = new LinkedHashMap<String, Object>();
                props.put("sasl.mechanism", r.mechanism());
                props.put("sasl.jaas.config", r.jaasConfig().reveal());
                yield props;
            }
            default -> throw new IllegalStateException("SaslConfig no soportado: " + sasl);
        };
    }

    private static Map<String, Object> withJaas(
            String mechanism, String loginModuleClass, List<JaasConfigBuilder.Entry> entries) {
        var props = new LinkedHashMap<String, Object>();
        props.put("sasl.mechanism", mechanism);
        props.put("sasl.jaas.config", JaasConfigBuilder.build(loginModuleClass, entries));
        return props;
    }

    private static Map<String, Object> mapOAuth(OAuthBearerSasl oauth) {
        var props = new LinkedHashMap<String, Object>();
        props.put("sasl.mechanism", "OAUTHBEARER");
        oauth.tokenEndpointUrl()
                .ifPresent(url -> props.put("sasl.oauthbearer.token.endpoint.url", url));
        props.put(
                "sasl.login.callback.handler.class",
                oauth.callbackHandlerClass()
                        .orElse(
                                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler"));
        var entries = new ArrayList<JaasConfigBuilder.Entry>();
        oauth.clientId()
                .ifPresent(id -> entries.add(JaasConfigBuilder.Entry.quoted("clientId", id)));
        oauth.clientSecret()
                .ifPresent(
                        secret ->
                                entries.add(
                                        JaasConfigBuilder.Entry.quoted(
                                                "clientSecret", secret.reveal())));
        oauth.scope()
                .ifPresent(scope -> entries.add(JaasConfigBuilder.Entry.quoted("scope", scope)));
        oauth.extensions()
                .forEach(
                        (key, value) ->
                                entries.add(
                                        JaasConfigBuilder.Entry.quoted("extension_" + key, value)));
        oauth.options()
                .forEach((key, value) -> entries.add(JaasConfigBuilder.Entry.quoted(key, value)));
        props.put(
                "sasl.jaas.config",
                JaasConfigBuilder.build(
                        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
                        entries));
        return props;
    }

    private static Map<String, Object> mapGssapi(GssapiSasl gssapi) {
        var props = new LinkedHashMap<String, Object>();
        props.put("sasl.mechanism", "GSSAPI");
        props.put("sasl.kerberos.service.name", gssapi.serviceName());
        var entries = new ArrayList<JaasConfigBuilder.Entry>();
        if (gssapi.useTicketCache()) {
            entries.add(JaasConfigBuilder.Entry.raw("useTicketCache", "true"));
        } else {
            entries.add(JaasConfigBuilder.Entry.raw("useKeyTab", "true"));
            entries.add(JaasConfigBuilder.Entry.raw("storeKey", "true"));
            gssapi.keytab()
                    .ifPresent(
                            keytab ->
                                    entries.add(JaasConfigBuilder.Entry.quoted("keyTab", keytab)));
        }
        entries.add(JaasConfigBuilder.Entry.quoted("principal", gssapi.principal()));
        props.put(
                "sasl.jaas.config",
                JaasConfigBuilder.build("com.sun.security.auth.module.Krb5LoginModule", entries));
        return props;
    }
}
