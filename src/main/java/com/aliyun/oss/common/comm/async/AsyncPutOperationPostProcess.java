package com.aliyun.oss.common.comm.async;

import com.aliyun.oss.internal.OSSUtils;
import com.aliyun.oss.model.PutObjectResult;

import java.io.IOException;

import static com.aliyun.oss.common.utils.LogUtils.logException;

public class AsyncPutOperationPostProcess extends AsyncPostProcess<PutObjectResult> {

    @Override
    public void postProcess(PutObjectResult putObjectResult, CallbackImpl callback) {
        try {
            if (callback.getClientConfiguration().isCrcCheckEnabled()) {
                OSSUtils.checkChecksum(putObjectResult.getClientCRC(), putObjectResult.getServerCRC(), putObjectResult.getRequestId());
            }
            callback.setWrappedResult(putObjectResult);
        } catch (Exception ex) {
            logException("[Post process]: ", ex,
                    callback.getRequestMessage().getOriginalRequest().isLogEnabled());
            callback.setException(ex);
        }
    }
}
