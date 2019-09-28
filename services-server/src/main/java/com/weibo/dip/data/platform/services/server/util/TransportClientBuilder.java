package com.weibo.dip.data.platform.services.server.util;

import com.floragunn.searchguard.SearchGuardPlugin;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.Collections;

public class TransportClientBuilder {

    private TransportClientBuilder() {

    }

    public static TransportClient build(String clusterName, String storePath, String keyStore, String trustStore,
                                        String keystorePassword, String trustStorePassword, String domain, int port) throws Exception {
        Settings.Builder settingsBuilder = Settings.builder();

        settingsBuilder.put("cluster.name", clusterName)
            .put("client.transport.sniff", true)
            .put("path.home", storePath)
            .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH, keyStore)
            .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_FILEPATH, trustStore)
            .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_PASSWORD, keystorePassword)
            .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_PASSWORD, trustStorePassword)
            .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLED, true)
            .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_TYPE, "JKS")
            .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_TYPE, "JKS")
            .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION, false);

        TransportClient client = new PreBuiltTransportClient(settingsBuilder.build(), Collections.singleton(SearchGuardPlugin.class));

        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(domain), 9300));

        return client;
    }

}
