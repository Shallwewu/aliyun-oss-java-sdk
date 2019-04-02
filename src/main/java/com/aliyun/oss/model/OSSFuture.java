package com.aliyun.oss.model;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.comm.async.AsyncOperationManager;
import com.aliyun.oss.common.comm.async.CallbackImpl;

import java.util.concurrent.CountDownLatch;

public class OSSFuture<T> {

    public T get() throws OSSException, ClientException {
        try {
            CountDownLatch latch = AsyncOperationManager.getLatch(this);
            latch.await();

            CallbackImpl<Object, T> callback = AsyncOperationManager.get(this);

            if (callback != null) {
                Exception ex = callback.getException();

                if (ex != null) {
                    if (ex instanceof ClientException) {
                        throw (ClientException) ex;
                    } else if (ex instanceof OSSException) {
                        throw (OSSException) ex;
                    }
                }
                return callback.getWrappedResult();
            }
            return null;
        } catch (InterruptedException iex) {
            throw new ClientException(iex.getMessage(), iex);
        } finally {
            AsyncOperationManager.delete(this);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        AsyncOperationManager.delete(this);
        super.finalize();
    }
}
