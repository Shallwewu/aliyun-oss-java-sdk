package com.aliyun.oss;

public interface AsyncHandler<RESULT> {
    void onError(Exception var1);

    void onSuccess(RESULT var1);
}
