package com.aliyun.oss.common.comm.async;


public class AsyncPostProcess<T> {

    public void postProcess(T response, CallbackImpl callback) {
        callback.setWrappedResult(response);
    }
}
