package com.aliyun.oss.integrationtests.async;

import com.aliyun.oss.*;
import com.aliyun.oss.model.*;
import junit.framework.Assert;
import org.junit.Test;

import java.io.File;
import java.util.*;

import static com.aliyun.oss.integrationtests.TestUtils.genFixedLengthFile;
import static com.aliyun.oss.integrationtests.TestUtils.removeFile;

public class AsyncPutObjectTest extends TestAsyncBase {

    class AsyncPutHandler implements AsyncHandler<PutObjectResult> {

        int mIndex;

        List<String> mList;

        public AsyncPutHandler(int index, List<String> list)
        {
            mIndex = index;
            mList = list;
        }

        @Override
        public void onError(Exception var1) {
            Assert.fail();
        }

        @Override
        public void onSuccess(PutObjectResult var1) {
            mList.set(mIndex, mIndex + "-finished");
        }
    }

    public void putFixedLengthFileAsync(int fileNum, long length) throws Exception {
        final String keyPrefix = "put-file-async-";
        LinkedList<OSSFuture<PutObjectResult>> tasks = new LinkedList<OSSFuture<PutObjectResult>>();
        List<String> filePaths = new LinkedList<String>();

        try {
            List<String> list = new LinkedList<String>();

            for (int i = 0; i < fileNum; i++) {
                final String filePath = genFixedLengthFile(length);
                filePaths.add(filePath);
            }

            for (int i = 0; i < fileNum; i++) {
                list.add(Integer.toString(i));
                AsyncHandler<PutObjectResult> handler = new AsyncPutHandler(i, list);

                PutObjectRequest request = new PutObjectRequest(bucketName, keyPrefix + i, new File(filePaths.get(i)));

                OSSFuture<PutObjectResult> task = ossAsyncClient.putObject(request, handler);
                tasks.add(task);
            }

            for (OSSFuture<PutObjectResult> task : tasks) {
                task.get();
            }

            for (int i = 0; i <list.size(); i++) {
                Assert.assertEquals(Integer.toString(i) + "-finished", list.get(i));
            }
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        } finally {
            for (String filePath : filePaths){
                removeFile(filePath);
            }
        }
    }

    @Test
    public void testPutObjectAsync() throws Exception {
        putFixedLengthFileAsync(100, 1 * 1024 * 1024);
        putFixedLengthFileAsync(10, 64 * 1024 * 1024);
        putFixedLengthFileAsync(2, 1 * 1024 * 1024 * 1024);
    }

    @Test
    public void testPutObjectFail() {
        final String keyPrefix = "put-object-async-fail";
        final int num = 30;
        Random random = new Random();
        List<OSSFuture<PutObjectResult>> tasks = new LinkedList<OSSFuture<PutObjectResult>>();
        List<String> list = new LinkedList<String>();
        List<String> filePaths = new LinkedList<String>();

        try {
            for (int i = 0; i < num; i++) {
                long inputStreamLength = random.nextInt(1024 * 1024);
                final String filePath = genFixedLengthFile(inputStreamLength);
                PutObjectRequest request = new PutObjectRequest(bucketName, keyPrefix + i, new File(filePath));
                AsyncFailHandler<PutObjectResult> handler = new AsyncFailHandler(i, list);

                OSSFuture<PutObjectResult> task = ossInvalidClient.putObject(request, handler);
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
        } catch (Exception ex) {
            Assert.fail();
        } finally {
            for (String filePath : filePaths){
                removeFile(filePath);
            }
        }
    }

}
