package com.aliyun.oss.common.comm;

import java.io.IOException;

public interface AsyncPostProcess<T> {

    public void postProcess(T response, CallbackImpl callback) throws IOException;
}
