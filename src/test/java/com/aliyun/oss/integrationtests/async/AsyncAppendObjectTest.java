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

public class AsyncAppendObjectTest extends TestAsyncBase {

    class AsyncCopyHandler implements AsyncHandler<AppendObjectResult> {

        long mPosition;

        public AsyncCopyHandler(long position)
        {
            mPosition = position;
        }

        @Override
        public void onError(Exception var1) {
            Assert.fail(var1.getMessage());
        }

        @Override
        public void onSuccess(AppendObjectResult var1) {
            Assert.assertEquals(mPosition, (long)var1.getNextPosition());
        }
    }

    @Test
    public void testAppendObjectAsync() {
        final String keyPrefix = "append-object-async";
        final int num = 30;
        Random random = new Random();
        List<OSSFuture<AppendObjectResult>> tasks = new LinkedList<OSSFuture<AppendObjectResult>>();
        List<Long> positions = new LinkedList<Long>();

        for (int i = 0; i < num; i++) {
            long inputStreamLength = random.nextInt(1024 * 1024);
            positions.add(inputStreamLength);
            AppendObjectRequest appendObjectRequest = new AppendObjectRequest(bucketName, keyPrefix + i, genFixedLengthInputStream(inputStreamLength));
            appendObjectRequest.setPosition(0L);
            AsyncCopyHandler handler = new AsyncCopyHandler(inputStreamLength);
            OSSFuture<AppendObjectResult> task = ossAsyncClient.appendObject(appendObjectRequest, handler);
            tasks.add(task);
        }

        for (int i = 0; i < num; i++) {
            OSSFuture<AppendObjectResult> task = tasks.get(i);
            AppendObjectResult result = task.get();
            Assert.assertEquals((long)positions.get(i), (long)(result.getNextPosition()));
        }
    }

    @Test
    public void testAppendObjectAsyncFail() {
        final String keyPrefix = "append-object-async";
        final int num = 30;
        Random random = new Random();
        List<OSSFuture<AppendObjectResult>> tasks = new LinkedList<OSSFuture<AppendObjectResult>>();
        List<String> list = new LinkedList<String>();

        for (int i = 0; i < num; i++) {
            list.add(Integer.toString(i));
            long inputStreamLength = random.nextInt(1024 * 1024);
            AppendObjectRequest appendObjectRequest = new AppendObjectRequest(bucketName, keyPrefix + i, genFixedLengthInputStream(inputStreamLength));
            appendObjectRequest.setPosition(0L);
            AsyncFailHandler<AppendObjectResult> handler = new AsyncFailHandler(i, list);
            OSSFuture<AppendObjectResult> task = ossInvalidClient.appendObject(appendObjectRequest, handler);
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
