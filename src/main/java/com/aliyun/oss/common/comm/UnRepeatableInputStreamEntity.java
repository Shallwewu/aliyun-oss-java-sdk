package com.aliyun.oss.common.comm;

import okhttp3.internal.http.UnrepeatableRequestBody;

public class UnRepeatableInputStreamEntity extends RepeatableInputStreamEntity implements UnrepeatableRequestBody {

    @Override
    public boolean isRepeatable() {
        return false;
    }

    public UnRepeatableInputStreamEntity(ServiceClient.Request request) {
        super(request);
    }
}
