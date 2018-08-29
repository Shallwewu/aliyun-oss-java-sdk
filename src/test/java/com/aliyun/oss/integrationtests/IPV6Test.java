package com.aliyun.oss.integrationtests;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.GetObjectRequest;
import junit.framework.Assert;
import org.junit.Test;

import java.io.File;

public class IPV6Test {

    private static final String endpoint = "<http://[2401:b180::de]>";

    private static final String accessKeyId = "<your accessKeyId";

    private static final String accessKeySecret = "<your accessKeySecret>";

    private static final String bucketName = "<your bucket name>";

    private static final String key = "hello_world";

    @Test
    public void testIPV6() {
        String fileDownload = "<your file name>";
        OSS ossClient = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret);

        try {
            ossClient.getObject(new GetObjectRequest(bucketName, key), new File(fileDownload));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            ossClient.shutdown();
        }
    }
}
