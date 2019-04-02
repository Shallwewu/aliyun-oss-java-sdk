package com.aliyun.oss.integrationtests.async;

import com.aliyun.oss.AsyncHandler;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.*;
import junit.framework.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static com.aliyun.oss.integrationtests.TestUtils.*;

public class AsyncGetObjectTest extends TestAsyncBase {

    class AsyncGetHandler implements AsyncHandler<OSSObject> {

        long mSize;

        public AsyncGetHandler(long size)
        {
            mSize = size;
        }

        @Override
        public void onError(Exception var1) {
            Assert.fail();
        }

        @Override
        public void onSuccess(OSSObject var1) {
            try {
                Assert.assertTrue(var1.getResponse().isSuccessful());
                Assert.assertEquals(mSize, var1.getObjectMetadata().getContentLength());

                int bytesRead;
                int totalBytes = 0;
                byte[] buffer = new byte[4096];
                while ((bytesRead = var1.getObjectContent().read(buffer)) != -1) {
                    totalBytes += bytesRead;
                }

                Assert.assertEquals(mSize, totalBytes);
            } catch (IOException ex) {
                Assert.fail(ex.getMessage());
            }
        }
    }

    @Test
    public void testGetObjectAsync() throws Exception {
        final String keyPrefix = "get-object-async";
        final int num = 30;
        Random random = new Random();
        LinkedList<OSSFuture<OSSObject>> tasks = new LinkedList<OSSFuture<OSSObject>>();

        List<Long> sizeList = new LinkedList<Long>();

        for (int i = 0; i < num; i++) {
            long inputStreamLength = random.nextInt(1024 * 1024);
            sizeList.add(inputStreamLength);
            ossSyncClient.putObject(bucketName, keyPrefix + i, genFixedLengthInputStream(inputStreamLength));
        }

        for (int i = 0; i < num; i++) {
            AsyncGetHandler handler = new AsyncGetHandler(sizeList.get(i));

            GetObjectRequest request = new GetObjectRequest(bucketName, keyPrefix + i);

            OSSFuture<OSSObject> task = ossAsyncClient.getObject(request, handler);
            tasks.add(task);
        }

        for (OSSFuture<OSSObject> task : tasks) {
            OSSObject result = task.get();
            InputStream inputStream = result.getObjectContent();
            Assert.assertTrue(inputStream != null);
            Assert.assertEquals(-1, inputStream.read());
            result.close();
        }
    }

    @Test
    public void testGetObjectAsyncFail() {
        final String keyPrefix = "get-object-async-fail";
        final int num = 30;
        Random random = new Random();
        List<OSSFuture<OSSObject>> tasks = new LinkedList<OSSFuture<OSSObject>>();
        List<String> list = new LinkedList<String>();

        for (int i = 0; i < num; i++) {
            long inputStreamLength = random.nextInt(1024 * 1024);
            ossSyncClient.putObject(bucketName, keyPrefix + i, genFixedLengthInputStream(inputStreamLength));
        }

        for (int i = 0; i < num; i++) {
            GetObjectRequest request = new GetObjectRequest(bucketName, keyPrefix + i);
            list.add(Integer.toString(i));
            AsyncFailHandler handler = new AsyncFailHandler<OSSObject>(i, list);
            OSSFuture<OSSObject> task = ossInvalidClient.getObject(request, handler);
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

    class AsyncGetToFileHandler implements AsyncHandler<ObjectMetadata> {

        long mSize;

        File mFile;

        public AsyncGetToFileHandler(long size, File file)
        {
            mSize = size;
            mFile = file;
        }

        @Override
        public void onError(Exception var1) {
            Assert.fail(var1.getMessage());
        }

        @Override
        public void onSuccess(ObjectMetadata var1) {
            Assert.assertEquals(mSize, mFile.length());
        }
    }

    @Test
    public void testGetObjectToFileAsync() {
        final String keyPrefix = "get-object-async";
        final int num = 30;
        Random random = new Random();
        LinkedList<OSSFuture<ObjectMetadata>> tasks = new LinkedList<OSSFuture<ObjectMetadata>>();
        ensureDirExist(DOWNLOAD_DIR);
        List<Long> list = new LinkedList<Long>();

        List<String> fileNameList = new LinkedList<String>();
        try {
            try {
                List<Long> sizeList = new LinkedList<Long>();
                List<File> fileList = new LinkedList<File>();

                for (int i = 0; i < num; i++) {
                    long inputStreamLength = random.nextInt(1024 * 1024);
                    list.add(inputStreamLength);
                    String fileName = DOWNLOAD_DIR + keyPrefix + i;
                    File file = new File(fileName);
                    file.createNewFile();
                    sizeList.add(inputStreamLength);
                    fileNameList.add(fileName);
                    fileList.add(file);
                    ossSyncClient.putObject(bucketName, keyPrefix + i, genFixedLengthInputStream(inputStreamLength));
                }

                for (int i = 0; i < num; i++) {
                    AsyncGetToFileHandler handler = new AsyncGetToFileHandler(sizeList.get(i), fileList.get(i));

                    GetObjectRequest request = new GetObjectRequest(bucketName, keyPrefix + i);

                    OSSFuture<ObjectMetadata> task = ossAsyncClient.getObject(request, fileList.get(i), handler);
                    tasks.add(task);
                }
            } catch (Exception ex) {
                Assert.fail(ex.getMessage());
            }

            for (int i = 0; i < tasks.size(); i++) {
                ObjectMetadata result = tasks.get(i).get();
                Assert.assertEquals((long) list.get(i), result.getContentLength());
            }
        } finally {
            for (String filePath : fileNameList){
                removeFile(filePath);
            }
        }
    }

    @Test
    public void testGetObjectToFileAsyncFail() {
        final String keyPrefix = "get-object-async-fail";
        final int num = 30;
        Random random = new Random();
        List<OSSFuture<ObjectMetadata>> tasks = new LinkedList<OSSFuture<ObjectMetadata>>();
        List<String> list = new LinkedList<String>();
        ensureDirExist(DOWNLOAD_DIR);

        List<String> fileNameList = new LinkedList<String>();
        try {
            try {
                List<File> fileList = new LinkedList<File>();

                for (int i = 0; i < num; i++) {
                    long inputStreamLength = random.nextInt(1024 * 1024);
                    String fileName = DOWNLOAD_DIR + keyPrefix + i;
                    File file = new File(fileName);
                    file.createNewFile();
                    fileNameList.add(fileName);
                    fileList.add(file);
                    ossSyncClient.putObject(bucketName, keyPrefix + i, genFixedLengthInputStream(inputStreamLength));
                }

                for (int i = 0; i < num; i++) {
                    GetObjectRequest request = new GetObjectRequest(bucketName, keyPrefix + i);
                    list.add(Integer.toString(i));
                    AsyncFailHandler handler = new AsyncFailHandler<ObjectMetadata>(i, list);
                    OSSFuture<ObjectMetadata> task = ossInvalidClient.getObject(request, fileList.get(i), handler);
                    tasks.add(task);
                }
            } catch (Exception ex) {
                Assert.fail(ex.getMessage());
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
        } finally {
            for (String filePath : fileNameList){
                removeFile(filePath);
            }
        }
    }
}
