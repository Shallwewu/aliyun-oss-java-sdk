package com.aliyun.oss.integrationtests.async;

import com.aliyun.oss.AsyncHandler;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.OSSFuture;
import com.aliyun.oss.model.PutObjectResult;
import junit.framework.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static com.aliyun.oss.integrationtests.TestUtils.genFixedLengthInputStream;

public class AsyncCopyObjectTest extends TestAsyncBase {

    class AsyncCopyHandler implements AsyncHandler<CopyObjectResult> {

        String mEtag;

        public AsyncCopyHandler(String etag)
        {
            mEtag = etag;
        }

        @Override
        public void onError(Exception var1) {
            Assert.fail(var1.getMessage());
        }

        @Override
        public void onSuccess(CopyObjectResult var1) {
            Assert.assertEquals(mEtag, var1.getETag());
        }
    }

    @Test
    public void testCopyObjectAsync() {
        final String keyPrefix = "copy-object-async";
        final int num = 30;
        Random random = new Random();
        List<OSSFuture<CopyObjectResult>> tasks = new LinkedList<OSSFuture<CopyObjectResult>>();
        List<String> etags = new LinkedList<String>();

        for (int i = 0; i < num; i++) {
            long inputStreamLength = random.nextInt(1024 * 1024);
            PutObjectResult putObjectResult = ossSyncClient.putObject(bucketName, keyPrefix + i, genFixedLengthInputStream(inputStreamLength));
            etags.add(putObjectResult.getETag());
        }

        for (int i = 0; i < num; i++) {
            AsyncCopyHandler handler = new AsyncCopyHandler(etags.get(i));
            CopyObjectRequest request = new CopyObjectRequest(bucketName, keyPrefix + i, bucketName, keyPrefix + "-dest" + i);
            OSSFuture<CopyObjectResult> task = ossAsyncClient.copyObject(request, handler);
            tasks.add(task);
        }

        for (int i = 0; i < num; i++) {
            CopyObjectResult result = tasks.get(i).get();
            Assert.assertEquals(etags.get(i), result.getETag());
        }
    }

    @Test
    public void testCopyObjectAsyncFail() {
        final String keyPrefix = "copy-object-async-fail";
        final int num = 30;
        Random random = new Random();
        List<OSSFuture<CopyObjectResult>> tasks = new LinkedList<OSSFuture<CopyObjectResult>>();
        List<String> list = new LinkedList<String>();

        for (int i = 0; i < num; i++) {
            long inputStreamLength = random.nextInt(1024 * 1024);
            ossSyncClient.putObject(bucketName, keyPrefix + i, genFixedLengthInputStream(inputStreamLength));
        }

        for (int i = 0; i < num; i++) {
            list.add(Integer.toString(i));
            AsyncFailHandler<CopyObjectResult> handler = new AsyncFailHandler(i, list);
            CopyObjectRequest request = new CopyObjectRequest(bucketName, keyPrefix + i, bucketName, keyPrefix + "-dest" + i);
            OSSFuture<CopyObjectResult> task = ossInvalidClient.copyObject(request, handler);
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
