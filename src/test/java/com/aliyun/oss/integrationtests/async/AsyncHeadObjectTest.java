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

public class AsyncHeadObjectTest extends TestAsyncBase {

    class AsyncHeadHandler implements AsyncHandler<ObjectMetadata> {

        String mEtag;

        public AsyncHeadHandler(String etag)
        {
            mEtag = etag;
        }

        @Override
        public void onError(Exception var1) {
            Assert.fail(var1.getMessage());
        }

        @Override
        public void onSuccess(ObjectMetadata var1) {
            Assert.assertEquals(mEtag, var1.getETag());
        }
    }

    @Test
    public void testHeadObjectAsync() {
        final String keyPrefix = "head-object-async";
        final int num = 30;
        Random random = new Random();
        List<OSSFuture<ObjectMetadata>> tasks = new LinkedList<OSSFuture<ObjectMetadata>>();
        List<String> etags = new LinkedList<String>();

        for (int i = 0; i < num; i++) {
            long inputStreamLength = random.nextInt(1024 * 1024);
            PutObjectResult putObjectResult = ossSyncClient.putObject(bucketName, keyPrefix + i, genFixedLengthInputStream(inputStreamLength));
            etags.add(putObjectResult.getETag());
        }

        for (int i = 0; i < num; i++) {
            AsyncHeadHandler handler = new AsyncHeadHandler(etags.get(i));
            HeadObjectRequest request = new HeadObjectRequest(bucketName, keyPrefix + i);
            OSSFuture<ObjectMetadata> task = ossAsyncClient.headObject(request, handler);
            tasks.add(task);
        }

        for (int i = 0; i < num; i++) {
            ObjectMetadata result = tasks.get(i).get();
            Assert.assertEquals(etags.get(i), result.getETag());
        }
    }

    class AsyncHeadObjectFailHandler implements AsyncHandler<ObjectMetadata> {

        int mIndex;

        List<String> mList;

        public AsyncHeadObjectFailHandler(int index, List<String> list)
        {
            mIndex = index;
            mList = list;
        }

        @Override
        public void onError(Exception var1) {
            OSSException var2 = (OSSException)var1;
            Assert.assertEquals("Unknown", var2.getErrorCode());
            mList.set(mIndex, mIndex + "-finished");
        }

        @Override
        public void onSuccess(ObjectMetadata var1) {
            Assert.fail();
        }
    }

    @Test
    public void testHeadObjectAsyncFail() {
        final String keyPrefix = "head-object-async-fail";
        final int num = 30;
        Random random = new Random();
        List<OSSFuture<ObjectMetadata>> tasks = new LinkedList<OSSFuture<ObjectMetadata>>();
        List<String> list = new LinkedList<String>();

        for (int i = 0; i < num; i++) {
            long inputStreamLength = random.nextInt(1024 * 1024);
            ossSyncClient.putObject(bucketName, keyPrefix + i, genFixedLengthInputStream(inputStreamLength));
        }

        for (int i = 0; i < num; i++) {
            list.add(Integer.toString(i));
            AsyncHeadObjectFailHandler handler = new AsyncHeadObjectFailHandler(i, list);
            HeadObjectRequest request = new HeadObjectRequest(bucketName, keyPrefix + i);
            OSSFuture<ObjectMetadata> task = ossInvalidClient.headObject(request, handler);
            tasks.add(task);
        }

        for (int i = 0; i < tasks.size(); i++) {
            try {
                tasks.get(i).get();
                Assert.fail();
            } catch (OSSException ex) {
                Assert.assertEquals("Unknown", ex.getErrorCode());
                Assert.assertEquals(i + "-finished", list.get(i));
            }
        }
    }
}
