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

import java.util.Map.Entry;

import okhttp3.MediaType;
import okhttp3.Request;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.common.utils.HttpHeaders;
import okhttp3.RequestBody;
import okio.BufferedSink;

class HttpRequestFactory {

    public Request createHttpRequest(ServiceClient.Request request, ExecutionContext context) {
        Request.Builder builder = createHttpRequestBuilder(request, context);

        return builder.build();
    }

    private Request.Builder createHttpRequestBuilder(ServiceClient.Request request, ExecutionContext context) {
        Request.Builder builder = new Request.Builder();
        String uri = request.getUri();

        HttpMethod method = request.getMethod();
        if (method == HttpMethod.POST) {
            RepeatableInputStreamEntity requestBody = null;

            if (request.getContent() != null) {
                if (request.getContent().markSupported()) {
                    requestBody = new RepeatableInputStreamEntity(request);
                } else {
                    requestBody = new UnRepeatableInputStreamEntity(request);
                }
            }
            builder.post(requestBody);
        } else if (method == HttpMethod.PUT) {
            RequestBody requestBody;

            if (request.getContent() != null) {
                if (request.isUseChunkEncoding()) {
                    builder.header(HttpHeaders.TRANSFER_ENCODING, "chunked");
                }
                if (request.getContent().markSupported()) {
                    requestBody = new RepeatableInputStreamEntity(request);
                } else {
                    requestBody = new UnRepeatableInputStreamEntity(request);
                }
            } else {
                requestBody = new RequestBody() {
                    @Override
                    public MediaType contentType() {
                        return null;
                    }

                    @Override
                    public void writeTo(BufferedSink sink) {
                    }
                };
            }
            builder.put(requestBody);
        } else if (method == HttpMethod.GET) {
            builder.get();
        } else if (method == HttpMethod.DELETE) {
            builder.delete();
        } else if (method == HttpMethod.HEAD) {
            builder.head();
        } else if (method == HttpMethod.OPTIONS) {
            builder.method("OPTIONS", null);
        } else {
            throw new ClientException("Unknown HTTP method name: " + method.toString());
        }

        builder.url(uri);
        configureRequestHeaders(request, context, builder);

        return builder;
    }

    private void configureRequestHeaders(ServiceClient.Request request, ExecutionContext context,
                                         Request.Builder builder) {

        for (Entry<String, String> entry : request.getHeaders().entrySet()) {
            if (entry.getKey().equalsIgnoreCase(HttpHeaders.CONTENT_LENGTH)
                    || entry.getKey().equalsIgnoreCase(HttpHeaders.HOST)) {
                continue;
            }

            builder.addHeader(entry.getKey(), entry.getValue());
        }
    }

}
