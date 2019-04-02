package com.aliyun.oss;

import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.common.comm.DefaultServiceClient;
import com.aliyun.oss.common.comm.ResponseMessage;
import com.aliyun.oss.common.comm.ServiceClient;
import com.aliyun.oss.common.comm.SignVersion;
import com.aliyun.oss.model.*;
import com.aliyun.oss.internal.*;

import java.io.File;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import static com.aliyun.oss.common.utils.LogUtils.logException;

public class OSSAsyncClient implements OSSAsync {

    /* The default credentials provider */
    private CredentialsProvider credsProvider;

    /* The valid endpoint for accessing to OSS services */
    private URI endpoint;

    /* The default service client */
    private ServiceClient serviceClient;

    /* The miscellaneous OSS operations */
    private OSSBucketOperation bucketOperation;
    private OSSObjectOperation objectOperation;

    /**
     * Uses the specified {@link CredentialsProvider}, client configuration and
     * OSS endpoint to create a new {@link OSSAsyncClient} instance.
     *
     * @param endpoint
     *            OSS services Endpoint.
     * @param credsProvider
     *            Credentials provider.
     * @param config
     *            client configuration.
     */
    public OSSAsyncClient(String endpoint, CredentialsProvider credsProvider, ClientConfiguration config) {
        this.credsProvider = credsProvider;
        config = config == null ? new ClientConfiguration() : config;
        this.serviceClient = new DefaultServiceClient(config);
        initOperations();
        setEndpoint(endpoint);
    }

    public OSSAsyncClient(String endpoint, String accessKeyId, String secretAccessKey) {
        this(endpoint, new DefaultCredentialProvider(accessKeyId, secretAccessKey), null);
    }

    public OSSAsyncClient(String endpoint, String accessKeyId, String secretAccessKey, ClientConfiguration config) {
        this(endpoint, new DefaultCredentialProvider(accessKeyId, secretAccessKey), config);
    }

    /**
     * Sets OSS services endpoint.
     *
     * @param endpoint
     *            OSS services endpoint.
     */
    public synchronized void setEndpoint(String endpoint) {
        URI uri = toURI(endpoint);
        this.endpoint = uri;

        if (isIpOrLocalhost(uri)) {
            serviceClient.getClientConfiguration().setSLDEnabled(true);
        }

        this.bucketOperation.setEndpoint(uri);
        this.objectOperation.setEndpoint(uri);
    }

    /**
     * Checks if the uri is an IP or domain. If it's IP or local host, then it
     * will use secondary domain of Alibaba cloud. Otherwise, it will use domain
     * directly to access the OSS.
     *
     * @param uri
     *            URI。
     */
    private boolean isIpOrLocalhost(URI uri) {
        if (uri.getHost().equals("localhost")) {
            return true;
        }

        InetAddress ia;
        try {
            ia = InetAddress.getByName(uri.getHost());
        } catch (UnknownHostException e) {
            return false;
        }

        if (uri.getHost().equals(ia.getHostAddress())) {
            return true;
        }

        return false;
    }

    private URI toURI(String endpoint) throws IllegalArgumentException {
        if (!endpoint.contains("://")) {
            ClientConfiguration conf = this.serviceClient.getClientConfiguration();
            endpoint = conf.getProtocol().toString() + "://" + endpoint;
        }

        try {
            return new URI(endpoint);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private void initOperations() {
        this.bucketOperation = new OSSBucketOperation(this.serviceClient, this.credsProvider);
        this.objectOperation = new OSSObjectOperation(this.serviceClient, this.credsProvider);
    }

    @Override
    public void switchCredentials(Credentials creds) {
        if (creds == null) {
            throw new IllegalArgumentException("creds should not be null.");
        }

        this.credsProvider.setCredentials(creds);
    }

    @Override
    public void switchSignatureVersion(SignVersion signatureVersion) {
        if (signatureVersion == null) {
            throw new IllegalArgumentException("signatureVersion should not be null.");
        }

        this.getClientConfiguration().setSignatureVersion(signatureVersion);
    }

    @Override
    public OSSFuture<PutObjectResult> putObject(PutObjectRequest putObjectRequest, AsyncHandler<PutObjectResult> handler)
            throws ClientException
    {
        return objectOperation.asyncPutObject(putObjectRequest, handler);
    }

    @Override
    public OSSFuture<ObjectMetadata> getObject(GetObjectRequest getObjectRequest, File file, AsyncHandler<ObjectMetadata> handler)
            throws ClientException {
        return objectOperation.asyncGetObject(getObjectRequest, file, handler);
    }

    @Override
    public OSSFuture<OSSObject> getObject(GetObjectRequest getObjectRequest, AsyncHandler<OSSObject> handler)
            throws ClientException {
        return objectOperation.asyncGetObject(getObjectRequest, handler);
    }

    @Override
    public OSSFuture<CopyObjectResult> copyObject(CopyObjectRequest copyObjectRequest, AsyncHandler<CopyObjectResult> handler)
            throws ClientException {
        return objectOperation.asyncCopyObject(copyObjectRequest, handler);
    }

    @Override
    public OSSFuture<AppendObjectResult> appendObject(AppendObjectRequest appendObjectRequest, AsyncHandler<AppendObjectResult> handler)
            throws ClientException {
        return objectOperation.asyncAppendObject(appendObjectRequest, handler);
    }

    @Override
    public OSSFuture<SimplifiedObjectMeta> getSimplifiedObjectMeta(GenericRequest genericRequest, AsyncHandler<SimplifiedObjectMeta> handler)
            throws ClientException {
        return objectOperation.asyncGetSimplifiedObjectMeta(genericRequest, handler);
    }

    @Override
    public OSSFuture<ObjectMetadata> headObject(HeadObjectRequest headObjectRequest, AsyncHandler<ObjectMetadata> handler)
            throws ClientException {
        return objectOperation.asyncHeadObject(headObjectRequest, handler);
    }

    @Override
    public OSSFuture<ObjectListing> listObjects(ListObjectsRequest listObjectsRequest, AsyncHandler<ObjectListing> handler)
            throws ClientException {
        return bucketOperation.asyncListObjects(listObjectsRequest, handler);
    }

    public CredentialsProvider getCredentialsProvider() {
        return this.credsProvider;
    }

    public ClientConfiguration getClientConfiguration() {
        return serviceClient.getClientConfiguration();
    }

    @Override
    public void shutdown() {
        try {
            serviceClient.shutdown();
        } catch (Exception e) {
            logException("shutdown throw exception: ", e);
        }
    }
}
