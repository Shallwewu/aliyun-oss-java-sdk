package com.aliyun.oss.integrationtests.async;

import com.aliyun.oss.*;
import com.aliyun.oss.common.utils.HttpUtil;
import com.aliyun.oss.integrationtests.TestConfig;
import com.aliyun.oss.model.*;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.util.*;

import static com.aliyun.oss.integrationtests.TestUtils.waitForCacheExpiration;
import static com.aliyun.oss.model.DeleteObjectsRequest.DELETE_OBJECTS_ONETIME_LIMIT;

public class TestAsyncBase {
    protected static final String BUCKET_NAME_PREFIX = "oss-async-api-";
    protected static final String DEFAULT_ENCODING_TYPE = "url";
    protected static final String USER_DIR = System.getProperty("user.dir");
    protected static final String DOWNLOAD_DIR = USER_DIR + File.separator + "download" + File.separator;

    protected static String bucketName;

    protected static OSS ossSyncClient;

    protected static OSSAsync ossAsyncClient;

    protected static OSSAsync ossInvalidClient;

    @BeforeClass
    public static void oneTimeSetUp() {
        ossSyncClient = new OSSClientBuilder().build(TestConfig.OSS_TEST_ENDPOINT, TestConfig.OSS_TEST_ACCESS_KEY_ID, TestConfig.OSS_TEST_ACCESS_KEY_SECRET);
        ossAsyncClient = new OSSAsyncClient(TestConfig.OSS_TEST_ENDPOINT, TestConfig.OSS_TEST_ACCESS_KEY_ID, TestConfig.OSS_TEST_ACCESS_KEY_SECRET);
        ossInvalidClient = new OSSAsyncClient(TestConfig.OSS_TEST_ENDPOINT, TestConfig.OSS_TEST_ACCESS_KEY_ID, TestConfig.OSS_TEST_ACCESS_KEY_SECRET.toLowerCase());

        cleanUpAllBuckets(ossSyncClient, BUCKET_NAME_PREFIX);
    }

    @Before
    public void setUp() throws Exception {
        long ticks = new Date().getTime() / 1000 + new Random().nextInt(5000);
        bucketName = BUCKET_NAME_PREFIX + ticks;
        createBucket(bucketName);
    }

    @After
    public void tearDown() throws Exception {
        deleteBucketWithObjects(ossSyncClient, bucketName);
        cleanUp();
    }

    public static void createBucket(String bucketName) {
        ossSyncClient.createBucket(bucketName);
        waitForCacheExpiration(2);
    }

    protected static void cleanUpAllBuckets(OSS client, String bucketPrefix) {
        List<String> bkts = listAllBuckets(client, bucketPrefix);
        for (String b : bkts) {
            deleteBucketWithObjects(client, b);
        }
    }

    protected static List<String> listAllBuckets(OSS client, String bucketPrefix) {
        List<String> bkts = new ArrayList<String>();
        String nextMarker = null;
        BucketList bucketList = null;

        do {
            ListBucketsRequest listBucketsRequest = new ListBucketsRequest(bucketPrefix, nextMarker,
                    ListBucketsRequest.MAX_RETURNED_KEYS);
            bucketList = client.listBuckets(listBucketsRequest);
            nextMarker = bucketList.getNextMarker();
            for (Bucket b : bucketList.getBucketList()) {
                bkts.add(b.getName());
            }
        } while (bucketList.isTruncated());

        return bkts;
    }

    protected static void deleteBucketWithObjects(OSS client, String bucketName) {
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

        // delete live channels
        List<LiveChannel> channels = client.listLiveChannels(bucketName);
        for (LiveChannel channel : channels) {
            client.deleteLiveChannel(bucketName, channel.getName());
        }

        // delete bucket
        client.deleteBucket(bucketName);
    }

    protected static List<String> listAllObjects(OSS client, String bucketName) {
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

    public static void cleanUp() {
        if (ossSyncClient != null) {
            ossSyncClient.shutdown();
            ossSyncClient = null;
        }

        if (ossAsyncClient != null) {
            ossAsyncClient.shutdown();
            ossAsyncClient = null;
        }

        if (ossInvalidClient != null) {
            ossInvalidClient.shutdown();
            ossInvalidClient = null;
        }
    }

    class AsyncFailHandler<RESULT> implements AsyncHandler<RESULT> {

        int mIndex;

        List<String> mList;

        public AsyncFailHandler(int index, List<String> list)
        {
            mIndex = index;
            mList = list;
        }

        @Override
        public void onError(Exception var1) {
            OSSException var2 = (OSSException)var1;
            Assert.assertEquals("SignatureDoesNotMatch", var2.getErrorCode());
            mList.set(mIndex, mIndex + "-finished");
        }

        @Override
        public void onSuccess(RESULT var1) {
            Assert.fail();
        }
    }
}
