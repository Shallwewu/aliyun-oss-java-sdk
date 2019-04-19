package com.aliyun.oss;

import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;

public class OSSClientAsyncClientBuilder {

    public OSSAsync build(String endpoint, String accessKeyId, String secretAccessKey) {
        return new OSSAsyncClient(endpoint, getDefaultCredentialProvider(accessKeyId, secretAccessKey),
                getClientConfiguration());
    }

    public OSSAsync build(String endpoint, String accessKeyId, String secretAccessKey, String securityToken) {
        return new OSSAsyncClient(endpoint, getDefaultCredentialProvider(accessKeyId, secretAccessKey, securityToken),
                getClientConfiguration());
    }

    public OSSAsync build(String endpoint, String accessKeyId, String secretAccessKey, ClientBuilderConfiguration config) {
        return new OSSAsyncClient(endpoint, getDefaultCredentialProvider(accessKeyId, secretAccessKey),
                getClientConfiguration(config));
    }

    public OSSAsync build(String endpoint, String accessKeyId, String secretAccessKey, String securityToken,
                     ClientBuilderConfiguration config) {
        return new OSSAsyncClient(endpoint, getDefaultCredentialProvider(accessKeyId, secretAccessKey, securityToken),
                getClientConfiguration(config));
    }

    public OSSAsync build(String endpoint, CredentialsProvider credsProvider) {
        return new OSSAsyncClient(endpoint, credsProvider, getClientConfiguration());
    }

    public OSSAsync build(String endpoint, CredentialsProvider credsProvider, ClientBuilderConfiguration config) {
        return new OSSAsyncClient(endpoint, credsProvider, getClientConfiguration(config));
    }

    private static ClientBuilderConfiguration getClientConfiguration() {
        return new ClientBuilderConfiguration();
    }

    private static ClientBuilderConfiguration getClientConfiguration(ClientBuilderConfiguration config) {
        if (config == null) {
            config = new ClientBuilderConfiguration();
        }
        return config;
    }

    private static DefaultCredentialProvider getDefaultCredentialProvider(String accessKeyId, String secretAccessKey) {
        return new DefaultCredentialProvider(accessKeyId, secretAccessKey);
    }

    private static DefaultCredentialProvider getDefaultCredentialProvider(String accessKeyId, String secretAccessKey,
                                                                          String securityToken) {
        return new DefaultCredentialProvider(accessKeyId, secretAccessKey, securityToken);
    }
}
