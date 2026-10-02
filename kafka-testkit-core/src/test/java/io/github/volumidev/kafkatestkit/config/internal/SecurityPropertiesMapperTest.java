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

import io.github.volumidev.kafkatestkit.config.GssapiSasl;
import io.github.volumidev.kafkatestkit.config.OAuthBearerSasl;
import io.github.volumidev.kafkatestkit.config.PlainSasl;
import io.github.volumidev.kafkatestkit.config.RawJaasSasl;
import io.github.volumidev.kafkatestkit.config.ScramMechanism;
import io.github.volumidev.kafkatestkit.config.ScramSasl;
import io.github.volumidev.kafkatestkit.config.Secret;
import io.github.volumidev.kafkatestkit.config.SecurityConfig;
import io.github.volumidev.kafkatestkit.config.SecurityProtocol;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SecurityPropertiesMapperTest {

    private final ResourceLoader resourceLoader = new ResourceLoader();

    @Test
    void plaintext_onlySetsProtocol() {
        var security =
                new SecurityConfig(SecurityProtocol.PLAINTEXT, Optional.empty(), Optional.empty());

        var props = SecurityPropertiesMapper.toKafkaProperties(security, resourceLoader);

        assertThat(props)
                .containsExactlyInAnyOrderEntriesOf(Map.of("security.protocol", "PLAINTEXT"));
    }

    @Test
    void plain_buildsEscapedJaas() {
        var security =
                securityWithSasl(SecurityProtocol.SASL_SSL, new PlainSasl("u", Secret.of("p\"x")));

        var props = SecurityPropertiesMapper.toKafkaProperties(security, resourceLoader);

        assertThat(props)
                .containsEntry("sasl.mechanism", "PLAIN")
                .containsEntry(
                        "sasl.jaas.config",
                        "org.apache.kafka.common.security.plain.PlainLoginModule required"
                                + " username=\"u\" password=\"p\\\"x\";");
    }

    @Test
    void scram512_buildsExpectedJaas() {
        var security =
                securityWithSasl(
                        SecurityProtocol.SASL_SSL,
                        new ScramSasl(ScramMechanism.SCRAM_SHA_512, "u", Secret.of("p")));

        var props = SecurityPropertiesMapper.toKafkaProperties(security, resourceLoader);

        assertThat(props)
                .containsEntry("sasl.mechanism", "SCRAM-SHA-512")
                .containsEntry(
                        "sasl.jaas.config",
                        "org.apache.kafka.common.security.scram.ScramLoginModule required"
                                + " username=\"u\" password=\"p\";");
    }

    @Test
    void oauthBearer_buildsTokenEndpointAndJaas() {
        var oauth =
                new OAuthBearerSasl(
                        Optional.of("https://idp/token"),
                        Optional.of("client-id"),
                        Optional.of(Secret.of("client-secret")),
                        Optional.of("kafka"),
                        Map.of("logicalCluster", "lkc-abc"),
                        Optional.empty(),
                        Map.of());
        var security = securityWithSasl(SecurityProtocol.SASL_SSL, oauth);

        var props = SecurityPropertiesMapper.toKafkaProperties(security, resourceLoader);

        assertThat(props)
                .containsEntry("sasl.mechanism", "OAUTHBEARER")
                .containsEntry("sasl.oauthbearer.token.endpoint.url", "https://idp/token")
                .containsEntry(
                        "sasl.login.callback.handler.class",
                        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler");
        assertThat((String) props.get("sasl.jaas.config"))
                .contains("clientId=\"client-id\"")
                .contains("clientSecret=\"client-secret\"")
                .contains("scope=\"kafka\"")
                .contains("extension_logicalCluster=\"lkc-abc\"");
    }

    @Test
    void gssapiWithKeytab_buildsExpectedJaas() {
        var kerberos =
                new GssapiSasl(
                        "kafka",
                        "qa-user@ACME.COM",
                        Optional.of("/etc/qa.keytab"),
                        Optional.empty(),
                        false);
        var security = securityWithSasl(SecurityProtocol.SASL_SSL, kerberos);

        var props = SecurityPropertiesMapper.toKafkaProperties(security, resourceLoader);

        assertThat(props).containsEntry("sasl.mechanism", "GSSAPI");
        assertThat(props).containsEntry("sasl.kerberos.service.name", "kafka");
        assertThat((String) props.get("sasl.jaas.config"))
                .isEqualTo(
                        "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true"
                                + " storeKey=true keyTab=\"/etc/qa.keytab\""
                                + " principal=\"qa-user@ACME.COM\";");
    }

    @Test
    void rawJaas_passesLiteralThrough() {
        var security =
                securityWithSasl(
                        SecurityProtocol.SASL_SSL,
                        new RawJaasSasl(
                                "AWS_MSK_IAM",
                                Secret.of(
                                        "software.amazon.msk.auth.iam.IAMLoginModule required;")));

        var props = SecurityPropertiesMapper.toKafkaProperties(security, resourceLoader);

        assertThat(props)
                .containsEntry("sasl.mechanism", "AWS_MSK_IAM")
                .containsEntry(
                        "sasl.jaas.config",
                        "software.amazon.msk.auth.iam.IAMLoginModule required;");
    }

    @Test
    void mtls_mapsTruststoreAndKeystore() {
        var security =
                new SecurityConfig(
                        SecurityProtocol.SSL,
                        Optional.of(
                                new io.github.volumidev.kafkatestkit.config.SslConfig(
                                        Optional.of(
                                                new io.github.volumidev.kafkatestkit.config
                                                        .StoreConfig(
                                                        "/certs/ca.p12",
                                                        Secret.of("ts-pass"),
                                                        Optional.empty(),
                                                        io.github.volumidev.kafkatestkit.config
                                                                .StoreType.PKCS12)),
                                        Optional.of(
                                                new io.github.volumidev.kafkatestkit.config
                                                        .StoreConfig(
                                                        "/certs/client.p12",
                                                        Secret.of("ks-pass"),
                                                        Optional.of(Secret.of("key-pass")),
                                                        io.github.volumidev.kafkatestkit.config
                                                                .StoreType.PKCS12)),
                                        Optional.empty(),
                                        "https",
                                        Optional.empty())),
                        Optional.empty());

        var props = SecurityPropertiesMapper.toKafkaProperties(security, resourceLoader);

        assertThat(props)
                .containsEntry("ssl.truststore.location", "/certs/ca.p12")
                .containsEntry("ssl.truststore.password", "ts-pass")
                .containsEntry("ssl.truststore.type", "PKCS12")
                .containsEntry("ssl.keystore.location", "/certs/client.p12")
                .containsEntry("ssl.keystore.password", "ks-pass")
                .containsEntry("ssl.key.password", "key-pass")
                .containsEntry("ssl.keystore.type", "PKCS12")
                .containsEntry("ssl.endpoint.identification.algorithm", "https");
    }

    private static SecurityConfig securityWithSasl(
            SecurityProtocol protocol, io.github.volumidev.kafkatestkit.config.SaslConfig sasl) {
        return new SecurityConfig(protocol, Optional.empty(), Optional.of(sasl));
    }
}
