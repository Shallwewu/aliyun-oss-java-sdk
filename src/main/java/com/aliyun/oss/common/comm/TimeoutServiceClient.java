/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.oss.common.comm;

import static com.aliyun.oss.common.utils.LogUtils.getLog;
import static com.aliyun.oss.common.utils.LogUtils.logException;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import okhttp3.Call;
import okhttp3.Request;
import okhttp3.Response;

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.utils.ExceptionFactory;

/**
 * Default implementation of {@link ServiceClient}.
 */
public class TimeoutServiceClient extends DefaultServiceClient {
    protected ThreadPoolExecutor executor;

    public TimeoutServiceClient(ClientConfiguration config) {
        super(config);

        int processors = Runtime.getRuntime().availableProcessors();
        executor = new ThreadPoolExecutor(processors * 5, processors * 10, 60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(processors * 100), Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.CallerRunsPolicy());
        executor.allowCoreThreadTimeOut(true);
    }

    @Override
    public ResponseMessage sendRequestCore(ServiceClient.Request request, ExecutionContext context) throws IOException {
        okhttp3.Request httpRequest = httpRequestFactory.createHttpRequest(request, context);
        HttpRequestTask httpRequestTask = new HttpRequestTask(httpRequest);
        Future<Response> future = executor.submit(httpRequestTask);
        Response httpResponse;

        try {
            httpResponse = future.get(this.config.getRequestTimeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logException("[ExecutorService]The current thread was interrupted while waiting: ", e);

            httpRequestTask.cancel();
            throw new ClientException(e.getMessage(), e);
        } catch (ExecutionException e) {
            RuntimeException ex;
            httpRequestTask.cancel();

            if (e.getCause() instanceof IOException) {
                ex = ExceptionFactory.createNetworkException((IOException) e.getCause());
            } else {
                ex = new OSSException(e.getMessage(), e);
            }

            logException("[ExecutorService]The computation threw an exception: ", ex);
            throw ex;
        } catch (TimeoutException e) {
            logException("[ExecutorService]The wait " + this.config.getRequestTimeout() + " timed out: ", e);

            httpRequestTask.cancel();
            throw new ClientException(e.getMessage(), OSSErrorCode.REQUEST_TIMEOUT, "Unknown", e);
        }

        return buildResponse(request, httpResponse);
    }

    @Override
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(ClientConfiguration.DEFAULT_THREAD_POOL_WAIT_TIME, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(ClientConfiguration.DEFAULT_THREAD_POOL_WAIT_TIME,
                        TimeUnit.MILLISECONDS)) {
                    getLog().warn("Pool did not terminate in "
                            + ClientConfiguration.DEFAULT_THREAD_POOL_WAIT_TIME / 1000 + " seconds");
                }
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        super.shutdown();
    }

    class HttpRequestTask implements Callable<Response> {
        private okhttp3.Request httpRequest;
        private Call call;

        public HttpRequestTask(okhttp3.Request httpRequest) {
            this.httpRequest = httpRequest;
        }

        public void cancel() {
            call.cancel();
        }

        @Override
        public Response call() throws Exception {
            call = httpClient.newCall(httpRequest);

            return call.execute();
        }
    }

}
