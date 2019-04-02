package com.aliyun.oss.common.comm.async;

import com.aliyun.oss.common.utils.CRC64;
import com.aliyun.oss.internal.OSSUtils;
import com.aliyun.oss.model.AppendObjectRequest;
import com.aliyun.oss.model.AppendObjectResult;

import static com.aliyun.oss.common.utils.LogUtils.logException;

public class AsyncAppendOperationPostProcess extends AsyncPostProcess<AppendObjectResult> {

    AppendObjectRequest appendObjectRequest;

    public AsyncAppendOperationPostProcess(AppendObjectRequest appendObjectRequest) {
        this.appendObjectRequest = appendObjectRequest;
    }

    @Override
    public void postProcess(AppendObjectResult appendObjectResult, CallbackImpl callback) {
        try {
            if (appendObjectRequest.getInitCRC() != null && appendObjectResult.getClientCRC() != null) {
                appendObjectResult.setClientCRC(CRC64.combine(appendObjectRequest.getInitCRC(), appendObjectResult.getClientCRC(),
                        (appendObjectResult.getNextPosition() - appendObjectRequest.getPosition())));
            }

            if (callback.getClientConfiguration().isCrcCheckEnabled() && appendObjectRequest.getInitCRC() != null) {
                OSSUtils.checkChecksum(appendObjectResult.getClientCRC(), appendObjectResult.getServerCRC(), appendObjectResult.getRequestId());
            }
            callback.setWrappedResult(appendObjectResult);
        } catch (Exception ex) {
            logException("[Post process]: ", ex,
                    callback.getRequestMessage().getOriginalRequest().isLogEnabled());
            callback.setException(ex);
        }
    }
}
