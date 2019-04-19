package com.aliyun.oss.common.comm.async;

import com.aliyun.oss.model.OSSFuture;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class AsyncOperationManager {

    private static Map<OSSFuture, CallbackImpl> futureTaskMap = new ConcurrentHashMap<OSSFuture, CallbackImpl>();

    private AsyncOperationManager() {}

    public static void put(OSSFuture task, CallbackImpl callback) {
        futureTaskMap.put(task, callback);
    }

    public static CallbackImpl get(OSSFuture task) {
        return futureTaskMap.get(task);
    }

    public static CallbackImpl delete(OSSFuture task) {
        return futureTaskMap.remove(task);
    }

    public static CountDownLatch getLatch(OSSFuture task) {
        CallbackImpl callback = get(task);

        if (callback != null) {
            return callback.getLatch();
        }
        return null;
    }
}
