package com.aliyun.oss.common.comm;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class AsyncOperationHandler {

    private static Map<OSSFutureTask, CallbackImpl> futureTaskMap = new ConcurrentHashMap<OSSFutureTask, CallbackImpl>();

    private AsyncOperationHandler() {}

    public static void put(OSSFutureTask task, CallbackImpl callback) {
        futureTaskMap.put(task, callback);
    }

    public static CallbackImpl get(OSSFutureTask task) {
        return futureTaskMap.get(task);
    }

    public static CallbackImpl delete(OSSFutureTask task) {
        return futureTaskMap.remove(task);
    }

    public static CountDownLatch getLatch(OSSFutureTask task) {
        CallbackImpl callback = get(task);

        if (callback != null) {
            return callback.getLatch();
        }
        return null;
    }
}
