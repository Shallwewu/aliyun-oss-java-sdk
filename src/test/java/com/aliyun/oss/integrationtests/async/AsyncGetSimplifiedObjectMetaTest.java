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

public class AsyncGetSimplifiedObjectMetaTest extends TestAsyncBase {

    class AsyncGetObjectMetaHandler implements AsyncHandler<SimplifiedObjectMeta> {

        String mEtag;

        public AsyncGetObjectMetaHandler(String etag)
        {
            mEtag = etag;
        }

        @Override
        public void onError(Exception var1) {
            Assert.fail(var1.getMessage());
        }

        @Override
        public void onSuccess(SimplifiedObjectMeta var1) {
            Assert.assertEquals(mEtag, var1.getETag());
        }
    }

    @Test
    public void testGetObjectMetaAsync() {
        final String keyPrefix = "get-object-meta-async";
        final int num = 30;
        Random random = new Random();
        List<OSSFuture<SimplifiedObjectMeta>> tasks = new LinkedList<OSSFuture<SimplifiedObjectMeta>>();
        List<String> etags = new LinkedList<String>();

        for (int i = 0; i < num; i++) {
            long inputStreamLength = random.nextInt(1024 * 1024);
            PutObjectResult putObjectResult = ossSyncClient.putObject(bucketName, keyPrefix + i, genFixedLengthInputStream(inputStreamLength));
            etags.add(putObjectResult.getETag());
        }

        for (int i = 0; i < num; i++) {
            AsyncGetObjectMetaHandler handler = new AsyncGetObjectMetaHandler(etags.get(i));
            GenericRequest request = new GenericRequest(bucketName, keyPrefix + i);
            OSSFuture<SimplifiedObjectMeta> task = ossAsyncClient.getSimplifiedObjectMeta(request, handler);
            tasks.add(task);
        }

        for (int i = 0; i < num; i++) {
            SimplifiedObjectMeta result = tasks.get(i).get();
            Assert.assertEquals(etags.get(i), result.getETag());
        }
    }

    @Test
    public void testGetObjectMetaAsyncFail() {
        final String keyPrefix = "get-object-meta-async-fail";
        final int num = 30;
        Random random = new Random();
        List<OSSFuture<SimplifiedObjectMeta>> tasks = new LinkedList<OSSFuture<SimplifiedObjectMeta>>();
        List<String> list = new LinkedList<String>();

        for (int i = 0; i < num; i++) {
            long inputStreamLength = random.nextInt(1024 * 1024);
            ossSyncClient.putObject(bucketName, keyPrefix + i, genFixedLengthInputStream(inputStreamLength));
        }

        for (int i = 0; i < num; i++) {
            list.add(Integer.toString(i));
            AsyncFailHandler<SimplifiedObjectMeta> handler = new AsyncFailHandler(i, list);
            GenericRequest request = new GenericRequest(bucketName, keyPrefix + i);
            OSSFuture<SimplifiedObjectMeta> task = ossInvalidClient.getSimplifiedObjectMeta(request, handler);
            tasks.add(task);
        }

        for (int i = 0; i < tasks.size(); i++) {
            try {
                tasks.get(i).get();
                Assert.fail();
            } catch (OSSException ex) {
                Assert.assertEquals("SignatureDoesNotMatch", ex.getErrorCode());
                Assert.assertEquals(i + "-finished", list.get(i));
            }
        }
    }
}
