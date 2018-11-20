package com.aliyun.oss.common.comm;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSException;

import java.util.concurrent.CountDownLatch;

public class OSSFutureTask<T> {

    public T get() throws OSSException, ClientException {
        try {
            CountDownLatch latch = AsyncOperationHandler.getLatch(this);
            latch.await();

            CallbackImpl<T> callback = AsyncOperationHandler.delete(this);

            if (callback != null) {
                Exception ex = callback.getException();

                if (ex != null) {
                    if (ex instanceof ClientException) {
                        throw (ClientException) ex;
                    } else if (ex instanceof OSSException) {
                        throw (OSSException) ex;
                    }
                }
                return callback.getResult();
            }
            return null;
        } catch (InterruptedException iex) {
            throw new ClientException(iex.getMessage(), iex);
        } finally {
            AsyncOperationHandler.delete(this);
        }

    }
}
