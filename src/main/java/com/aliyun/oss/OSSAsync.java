package com.aliyun.oss;

import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.comm.ResponseMessage;
import com.aliyun.oss.common.comm.SignVersion;
import com.aliyun.oss.model.*;

import java.io.File;

public interface OSSAsync {

    /**
     * Switches to another users with specified credentials
     *
     * @param creds
     *            the credential to switch to。
     */
    public void switchCredentials(Credentials creds);

    /**
     * Switches to another signature version
     *
     * @param signatureVersion
     *            the signature version to switch to。
     */
    public void switchSignatureVersion(SignVersion signatureVersion);

    /**
     * Shuts down the OSS instance (release all resources) The OSS instance is
     * not usable after its shutdown() is called.
     */
    public void shutdown();

    /**
     * Uploads the file with {@link PutObjectRequest}, which is a asynchronous api, the result
     * will be ready after the asynchronous task has been completed, the caller could get the
     * result with {@link OSSFuture <PutObjectResult>}
     * @param putObjectRequest
     *            The {@link PutObjectRequest} instance that has bucket name,
     *            object key, metadata information.
     * @return  A {@link OSSFuture <PutObjectResult>} instance.
     * @throws OSSException
     * @throws ClientException
     */
    public OSSFuture<PutObjectResult> putObject(PutObjectRequest putObjectRequest, AsyncHandler<PutObjectResult> handler) throws ClientException;

    public OSSFuture<ObjectMetadata> getObject(GetObjectRequest getObjectRequest, File file, AsyncHandler<ObjectMetadata> handler) throws ClientException;

    /**
     * Gets the {@link OSSFuture <OSSObject>}, a asynchronous task for get
     * a object from the bucket, the caller could get the object later after the
     * asynchronous task has been completed.
     * @param getObjectRequest
     * @return  A {@link OSSFuture <OSSObject>} instance which the caller could get the result
     *          after the asynchronous task has been completed.
     * @throws OSSException
     * @throws ClientException
     */
    public OSSFuture<OSSObject> getObject(GetObjectRequest getObjectRequest, AsyncHandler<OSSObject> handler) throws ClientException;

    public OSSFuture<CopyObjectResult> copyObject(CopyObjectRequest copyObjectRequest, AsyncHandler<CopyObjectResult> handler) throws ClientException;

    public OSSFuture<AppendObjectResult> appendObject(AppendObjectRequest appendObjectRequest, AsyncHandler<AppendObjectResult> handler) throws ClientException;

    public OSSFuture<SimplifiedObjectMeta> getSimplifiedObjectMeta(GenericRequest genericRequest, AsyncHandler<SimplifiedObjectMeta> handler) throws ClientException;

    public OSSFuture<ObjectMetadata> headObject(HeadObjectRequest headObjectRequest, AsyncHandler<ObjectMetadata> handler) throws ClientException;

    public OSSFuture<ObjectListing> listObjects(ListObjectsRequest listObjectsRequest, AsyncHandler<ObjectListing> handler) throws ClientException;

}
