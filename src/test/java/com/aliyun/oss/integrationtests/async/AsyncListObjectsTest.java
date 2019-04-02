package com.aliyun.oss.integrationtests.async;

import com.aliyun.oss.AsyncHandler;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.*;
import junit.framework.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static com.aliyun.oss.integrationtests.TestUtils.genFixedLengthInputStream;

public class AsyncListObjectsTest extends TestAsyncBase {

    class AsyncListObjectsHandler implements AsyncHandler<ObjectListing> {

        List<String> mKeys;

        List<String> mEtags;

        public AsyncListObjectsHandler(List<String> keys, List<String> etags)
        {
            mKeys = keys;
            mEtags = etags;
        }

        @Override
        public void onError(Exception var1) {
            Assert.fail(var1.getMessage());
        }

        @Override
        public void onSuccess(ObjectListing var1) {
            List<OSSObjectSummary> summaries = var1.getObjectSummaries();

            for (int i = 0; i < summaries.size(); i++) {
                Assert.assertEquals(mKeys.get(i), summaries.get(i).getKey());
                Assert.assertEquals(mEtags.get(i), summaries.get(i).getETag());
            }
        }
    }

    @Test
    public void testListObjectsAsync() {
        final String keyPrefix = "list-objects-async";
        final int num = 10;
        Random random = new Random();
        List<String> keys = new LinkedList<String>();
        List<String> etags = new LinkedList<String>();
        deleteBucketWithObjects(ossSyncClient, bucketName);
        createBucket(bucketName);

        for (int i = 0; i < num; i++) {
            long inputStreamLength = random.nextInt(1024 * 1024);
            PutObjectResult putObjectResult = ossSyncClient.putObject(bucketName, keyPrefix + i, genFixedLengthInputStream(inputStreamLength));
            keys.add(keyPrefix + i);
            etags.add(putObjectResult.getETag());
        }

        for (int i = 0; i < num; i++) {
            long inputStreamLength = random.nextInt(1024 * 1024);
            PutObjectResult putObjectResult = ossSyncClient.putObject(bucketName, keyPrefix + "a" + i, genFixedLengthInputStream(inputStreamLength));
            keys.add(keyPrefix + "a" + i);
            etags.add(putObjectResult.getETag());
        }

        ListObjectsRequest request = new ListObjectsRequest(bucketName);
        AsyncListObjectsHandler handler = new AsyncListObjectsHandler(keys, etags);
        OSSFuture<ObjectListing> task = ossAsyncClient.listObjects(request, handler);
        ObjectListing result = task.get();

        List<OSSObjectSummary> summaries = result.getObjectSummaries();

        for (int i = 0; i < summaries.size(); i++) {
            Assert.assertEquals(keys.get(i), summaries.get(i).getKey());
            Assert.assertEquals(etags.get(i), summaries.get(i).getETag());
        }
    }

    @Test
    public void testListObjectsFailAsync() {
        final String keyPrefix = "list-objects-async";
        final int num = 30;
        Random random = new Random();
        deleteBucketWithObjects(ossSyncClient, bucketName);
        createBucket(bucketName);

        for (int i = 0; i < num; i++) {
            long inputStreamLength = random.nextInt(1024 * 1024);
            ossSyncClient.putObject(bucketName, keyPrefix + i, genFixedLengthInputStream(inputStreamLength));
        }

        ListObjectsRequest request = new ListObjectsRequest(bucketName);
        List<String> list = new LinkedList<String>();
        list.add("1");
        AsyncFailHandler<ObjectListing> handler = new AsyncFailHandler(0, list);
        OSSFuture<ObjectListing> task = ossInvalidClient.listObjects(request, handler);
        try {
            task.get();
            Assert.fail();
        } catch (OSSException ex) {
            Assert.assertEquals("SignatureDoesNotMatch", ex.getErrorCode());
            Assert.assertEquals("0-finished", list.get(0));
        }
    }
}
