package com.aliyun.oss.integrationtests;

import com.aliyun.oss.*;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.PutObjectRequest;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;

import static com.aliyun.oss.integrationtests.TestUtils.genFixedLengthInputStream;

public class NetTimeoutTest extends TestBase {

    @Test
    public void TestConnectionTimeout()
    {
        ClientBuilderConfiguration configuration = new ClientBuilderConfiguration();
        configuration.setConnectionTimeout(10000);
        configuration.setMaxErrorRetry(0);
        OSS client = new OSSClientBuilder().build("10.10.10.10", "111", "111", configuration);

        GetObjectRequest request1 = new GetObjectRequest("bucket", "object");
        long begin1 = System.currentTimeMillis();

        try {
            client.getObject(request1);
            Assert.fail("could not be here");
        } catch (ClientException ex) {
            long duration = System.currentTimeMillis() - begin1;
            Assert.assertTrue(duration > 10000);
            Assert.assertTrue(duration < 15000);
        }

        GetObjectRequest request2 = new GetObjectRequest("bucket", "object");
        request2.setConnectionTimeout(5000);
        long begin2 = System.currentTimeMillis();

        try {
            client.getObject(request2);
            Assert.fail("could not be here");
        } catch (ClientException ex) {
            long duration = System.currentTimeMillis() - begin2;
            Assert.assertTrue(duration > 5000);
            Assert.assertTrue(duration < 10000);
        }
    }

    @Test
    public void TestReadTimeout()
    {
        String key = "read-timeout";
        ClientBuilderConfiguration configuration = new ClientBuilderConfiguration();
        configuration.setRequestTimeout(50000);
        configuration.setMaxErrorRetry(0);
        OSS client = new OSSClientBuilder().build(TestConfig.OSS_TEST_ENDPOINT, TestConfig.OSS_TEST_ACCESS_KEY_ID, TestConfig.OSS_TEST_ACCESS_KEY_SECRET, configuration);

        final int instreamLength = 128 * 1024;
        InputStream instream = genFixedLengthInputStream(instreamLength);
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, instream);
        client.putObject(putObjectRequest);

        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
        getObjectRequest.setReadTimeout(1);
        long begin = System.currentTimeMillis();

        try {
            client.getObject(getObjectRequest);
            Assert.fail("could not be here");
        } catch (ClientException ex) {
            long duration = System.currentTimeMillis() - begin;
            Assert.assertTrue(duration > 1);
            Assert.assertTrue(duration < 1000);
        }
    }

    @Test
    public void TestWriteTimeout()
    {
        String key = "write-timeout";
        ClientBuilderConfiguration configuration = new ClientBuilderConfiguration();
        configuration.setWriteTimeout(50000);
        configuration.setMaxErrorRetry(0);
        OSS client = new OSSClientBuilder().build(TestConfig.OSS_TEST_ENDPOINT, TestConfig.OSS_TEST_ACCESS_KEY_ID, TestConfig.OSS_TEST_ACCESS_KEY_SECRET, configuration);

        final int instreamLength = 20 * 1024 * 1024;
        InputStream instream = genFixedLengthInputStream(instreamLength);
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, instream);
        putObjectRequest.setWriteTimeout(1);
        long begin = System.currentTimeMillis();

        try {
            client.putObject(putObjectRequest);
            Assert.fail("could not be here");
        } catch (ClientException ex) {
            long duration = System.currentTimeMillis() - begin;
            Assert.assertTrue(duration > 1);
            Assert.assertTrue(duration < 1000);
        }
    }
}
