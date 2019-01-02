package com.aliyun.oss.common.comm;

import com.aliyun.oss.internal.OSSUtils;
import com.aliyun.oss.model.PutObjectResult;

import java.io.IOException;

public class AsyncPutOperationPostProcess implements AsyncPostProcess<PutObjectResult> {

    @Override
    public void postProcess(PutObjectResult putObjectResult, CallbackImpl callback) throws IOException {
        if (callback.getClientConfiguration().isCrcCheckEnabled()) {
            OSSUtils.checkChecksum(putObjectResult.getClientCRC(), putObjectResult.getServerCRC(), putObjectResult.getRequestId());
        }
    }
}
