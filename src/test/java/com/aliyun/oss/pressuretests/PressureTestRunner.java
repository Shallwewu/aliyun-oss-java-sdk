package com.aliyun.oss.pressuretests;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.common.utils.HttpUtil;
import com.aliyun.oss.model.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

import static com.aliyun.oss.model.DeleteObjectsRequest.DELETE_OBJECTS_ONETIME_LIMIT;


public class PressureTestRunner {

    private static final Log log = LogFactory.getLog(PressureTestRunner.class);

    private static final String DEFAULT_ENCODING_TYPE = "url";
    private OSS ossClient;
    private final String endpoint = "<your endpoint>";
    private final String accessKeyId = "<you accessKeyId>";
    private final String accessKeySecret = "<your accessKeySecret>";
    private final String bucket = "<your bucket>";
    private final String keyPrefix = "pressure-test-";
    private byte[] byteArray;
    private final int contentLength = 4 * 1024;
    private final int transferNum = 10000;
    private final int putThreadNum = 10;
    private final int getThreadNum = 10;
    private long putApiCallTime = 0;
    private long getApiCallTime = 0;
    private long maxPutTime = 0;
    private long maxGetTime = 0;
    private int putErrNum = 0;
    private int getErrNum = 0;

    private final ReentrantLock lock = new ReentrantLock();

    private void prepareEnv() {
        try {
            ossClient = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret);
            ossClient.createBucket(bucket);
        } catch (Exception ex) {
            log.warn(ex.getMessage());
        }
    }

    private void testPutObject() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(putThreadNum, putThreadNum, 600L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(transferNum), Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.CallerRunsPolicy());
        byteArray = createFixedLengthBytes(contentLength);
        CountDownLatch latch = new CountDownLatch(transferNum);

        long begin = System.currentTimeMillis();

        for (int i = 0; i < transferNum; i++) {
            String key = keyPrefix + i;
            PutTask task = new PutTask(key, latch);
            executor.submit(task);
        }

        try {
            latch.await();

            long end = System.currentTimeMillis();
            long consume = end - begin;
            log.info("Put object : \nTotal time:" + consume + "\nMax time:" + maxPutTime + "\nAvg time:"
                    + (double)consume / transferNum + "\nAvg API call time:" + (double)putApiCallTime / transferNum
                    + "\nPutErrNum:" + putErrNum);
        } catch (InterruptedException ex) {
            log.info(ex);
        }
    }

    private void testGetObject() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(getThreadNum, getThreadNum, 600L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(transferNum), Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.CallerRunsPolicy());
        CountDownLatch latch = new CountDownLatch(transferNum);
        long begin = System.currentTimeMillis();

        for (int i = 0; i < transferNum; i++) {
            String key = keyPrefix + i;
            GetTask task = new GetTask(key, latch);
            executor.execute(task);
        }

        try {
            latch.await();

            long end = System.currentTimeMillis();
            long consume = end - begin;
            log.info("Get object : \nTotal time:" + consume + "\n Max time:" + maxGetTime + "\n Avg time:"
                    + (double)consume / transferNum + "\n Avg API call time:" + (double)getApiCallTime / transferNum
                    + "\nGetErrNum:" + getErrNum);
        } catch (InterruptedException ex) {
            log.info(ex);
        }
    }

    private class PutTask implements Runnable {
        String key;
        CountDownLatch latch;

        public PutTask(String key, CountDownLatch latch) {
            this.key = key;
            this.latch = latch;
        }

        @Override
        public void run() {
            InputStream input;
            try {
                input = new ByteArrayInputStream(byteArray);
                long begin = System.currentTimeMillis();
                ossClient.putObject(bucket, key, input);
                long end = System.currentTimeMillis();
                long consume = end - begin;
                lock.lock();
                maxPutTime = maxPutTime > consume ? maxPutTime : consume;
                putApiCallTime += consume;
                lock.unlock();
            } catch (Exception ex) {
                lock.lock();
                putErrNum++;
                lock.unlock();
                log.warn("Put object " + key + " , detail: " + ex.getMessage());
            } finally {
                latch.countDown();
            }
        }
    }

    private class GetTask implements Runnable {
        String key;
        CountDownLatch latch;

        public GetTask(String key, CountDownLatch latch) {
            this.key = key;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                long begin = System.currentTimeMillis();
                OSSObject o = ossClient.getObject(bucket, key);

                //read data
                byte[] content = new byte[4096];
                InputStream in = o.getObjectContent();

                while (in.read(content) >= 0) {
                }
                long end = System.currentTimeMillis();
                long consume = end - begin;
                lock.lock();
                maxGetTime = maxGetTime > consume ? maxGetTime : consume;
                getApiCallTime += consume;
                lock.unlock();
            } catch (Exception ex) {
                lock.lock();
                getErrNum++;
                lock.unlock();
                log.warn("Get object " + key + " , detail: " + ex.getMessage());
            } finally {
                latch.countDown();
            }
        }

    }

    private void clear() {
        deleteBucketWithObjects(ossClient, bucket);
        ossClient.shutdown();
    }

    private static List<String> listAllObjects(OSS client, String bucketName) {
        List<String> objs = new ArrayList<String>();
        ObjectListing objectListing = null;
        String nextMarker = null;

        do {
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucketName, null, nextMarker, null,
                    DELETE_OBJECTS_ONETIME_LIMIT);
            listObjectsRequest.setEncodingType(DEFAULT_ENCODING_TYPE);
            objectListing = client.listObjects(listObjectsRequest);
            if (DEFAULT_ENCODING_TYPE.equals(objectListing.getEncodingType())) {
                nextMarker = HttpUtil.urlDecode(objectListing.getNextMarker(), "UTF-8");
            } else {
                nextMarker = objectListing.getNextMarker();
            }

            List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
            for (OSSObjectSummary s : sums) {
                if (DEFAULT_ENCODING_TYPE.equals(objectListing.getEncodingType())) {
                    objs.add(HttpUtil.urlDecode(s.getKey(), "UTF-8"));
                } else {
                    objs.add(s.getKey());
                }
            }
        } while (objectListing.isTruncated());

        return objs;
    }

    private static void deleteBucketWithObjects(OSS client, String bucketName) {
        if (!client.doesBucketExist(bucketName)) {
            return;
        }

        // delete objects
        List<String> allObjects = listAllObjects(client, bucketName);
        int total = allObjects.size();
        if (total > 0) {
            int opLoops = total / DELETE_OBJECTS_ONETIME_LIMIT;
            if (total % DELETE_OBJECTS_ONETIME_LIMIT != 0) {
                opLoops++;
            }

            List<String> objectsToDel = null;
            for (int i = 0; i < opLoops; i++) {
                int fromIndex = i * DELETE_OBJECTS_ONETIME_LIMIT;
                int len = 0;
                if (total <= DELETE_OBJECTS_ONETIME_LIMIT) {
                    len = total;
                } else {
                    len = (i + 1 == opLoops) ? (total - fromIndex) : DELETE_OBJECTS_ONETIME_LIMIT;
                }
                objectsToDel = allObjects.subList(fromIndex, fromIndex + len);

                DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucketName);
                deleteObjectsRequest.setEncodingType(DEFAULT_ENCODING_TYPE);
                deleteObjectsRequest.setKeys(objectsToDel);
                client.deleteObjects(deleteObjectsRequest);
            }
        }

        // delete bucket
        client.deleteBucket(bucketName);
    }

    private byte[] createFixedLengthBytes(int fixedLength) {
        byte[] data = new byte[fixedLength];
        for (int i = 0; i < fixedLength; i++) {
            data[i] = 'a';
        }
        return data;
    }

    public static void main(String[] args) {
        PressureTestRunner runner = new PressureTestRunner();

        runner.prepareEnv();
        runner.testPutObject();
        runner.testGetObject();
        runner.clear();
    }
}