package com.aliyun.oss.common.comm;

import com.aliyun.oss.*;
import com.aliyun.oss.common.comm.async.AsyncOperationManager;
import com.aliyun.oss.common.comm.async.CallbackImpl;
import com.aliyun.oss.model.OSSFuture;
import com.aliyun.oss.common.utils.ExceptionFactory;
import com.aliyun.oss.common.utils.HttpHeaders;
import com.aliyun.oss.common.utils.HttpUtil;
import com.aliyun.oss.common.utils.IOUtils;
import okhttp3.*;

import javax.net.ssl.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.aliyun.oss.common.utils.LogUtils.logException;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;

public class DefaultServiceClient extends ServiceClient {

    protected static HttpRequestFactory httpRequestFactory = new HttpRequestFactory();

    protected OkHttpClient httpClient;

    public DefaultServiceClient(ClientConfiguration config) {
        super(config);
        ConnectionPool connectionPool = new ConnectionPool(config.getMaxIdleConnections(), config.getIdleConnectionTime(), TimeUnit.MILLISECONDS);
        OkHttpClient.Builder builder = new OkHttpClient.Builder().followRedirects(false).followSslRedirects(false)
                .connectTimeout(config.getConnectionTimeout(), TimeUnit.MILLISECONDS).readTimeout(config.getReadTimeout(), TimeUnit.MILLISECONDS)
                .writeTimeout(config.getWriteTimeout(), TimeUnit.MILLISECONDS).retryOnConnectionFailure(true).connectionPool(connectionPool);
        List<okhttp3.Protocol> protocols = new ArrayList<okhttp3.Protocol>();
        protocols.add(okhttp3.Protocol.HTTP_2);
        protocols.add(okhttp3.Protocol.HTTP_1_1);

        builder.protocols(protocols);

        builder.hostnameVerifier(new HostnameVerifier() {
            @Override
            public boolean verify(String hostname, SSLSession session) {
                return true;
            }
        });

        final TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) throws CertificateException {
                    }

                    @Override
                    public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) throws CertificateException {
                    }

                    @Override
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return new java.security.cert.X509Certificate[]{};
                    }
                }
        };

        SSLSocketFactory sslSocketFactory = null;

        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            sslSocketFactory = sslContext.getSocketFactory();
        } catch (Exception e) {
            e.printStackTrace();
        }

        builder.sslSocketFactory(sslSocketFactory, (X509TrustManager) trustAllCerts[0]);
        String proxyHost = config.getProxyHost();
        int proxyPort = config.getProxyPort();

        if (proxyHost != null && proxyPort > 0) {
            Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
            final String proxyUsername = config.getProxyUsername();
            final String proxyPassword = config.getProxyPassword();
            Authenticator authenticator = new Authenticator() {
                @Override
                public okhttp3.Request authenticate(Route route, Response response) throws IOException {
                    if (responseCount(response) >= 3) {
                        return null; // If we've failed 3 times, give up.
                    }
                    String credential = Credentials.basic(proxyUsername, proxyPassword);

                    return response.request().newBuilder().header("Proxy-Authorization", credential).build();
                }

                private int responseCount(Response response) {
                    int result = 1;
                    while ((response = response.priorResponse()) != null) {
                        result++;
                    }
                    return result;
                }
            };
            builder.proxy(proxy);
            builder.proxyAuthenticator(authenticator);
        }
        httpClient = builder.build();
        httpClient.dispatcher().setMaxRequests(config.getMaxConnections());
        httpClient.dispatcher().setMaxRequestsPerHost(config.getMaxConnections());
    }

    @Override
    protected ResponseMessage sendRequestCore(Request request, ExecutionContext context) throws IOException {
        okhttp3.Request httpRequest = httpRequestFactory.createHttpRequest(request, context);

        Response response;

        Call call = httpClient.newCall(httpRequest);
        try {
            response = call.execute();
        } catch (IOException ex) {
            call.cancel();
            throw ExceptionFactory.createNetworkException(ex);
        }

        return buildResponse(request, response);
    }

    @Override
    protected <T, RESULT> OSSFuture<RESULT> asyncSendRequestCore(Request request, ExecutionContext context, CallbackImpl<T, RESULT> callback) {
        okhttp3.Request httpRequest = httpRequestFactory.createHttpRequest(request, context);

        OSSFuture futureTask = new OSSFuture();
        callback.setRequest(request);
        callback.setContext(context);
        AsyncOperationManager.put(futureTask, callback);

        Call call = httpClient.newCall(httpRequest);

        call.enqueue(callback);

        return futureTask;
    }

    public static ResponseMessage buildResponse(ServiceClient.Request request, Response httpResponse)
            throws IOException {

        assert (httpResponse != null);

        ResponseMessage response = new ResponseMessage(request);
        response.setUrl(request.getUri());
        response.setResponse(httpResponse);

        response.setStatusCode(httpResponse.code());

        if (response.isSuccessful()) {
            response.setContent(httpResponse.body().byteStream());
        } else {
            readAndSetErrorResponse(httpResponse.body().byteStream(), response);
        }

        for (int index = 0; index < httpResponse.headers().size(); index++) {
            String name = httpResponse.headers().name(index);
            String value = httpResponse.headers().value(index);
            if (HttpHeaders.CONTENT_LENGTH.equals(name)) {
                response.setContentLength(Long.parseLong(value));
            }
            response.addHeader(name, value);
        }

        HttpUtil.convertHeaderCharsetFromIso88591(response.getHeaders());

        return response;
    }

    private static void readAndSetErrorResponse(InputStream originalContent, ResponseMessage response)
            throws IOException {
        byte[] contentBytes = IOUtils.readStreamAsByteArray(originalContent);
        response.setErrorResponseAsString(new String(contentBytes));
        response.setContent(new ByteArrayInputStream(contentBytes));
    }

    private static class DefaultRetryStrategy extends RetryStrategy {

        @Override
        public boolean shouldRetry(Exception ex, RequestMessage request, ResponseMessage response, int retries) {
            if (ex instanceof ClientException) {
                String errorCode = ((ClientException) ex).getErrorCode();
                if (errorCode.equals(ClientErrorCode.CONNECTION_TIMEOUT)
                        || errorCode.equals(ClientErrorCode.SOCKET_TIMEOUT)
                        || errorCode.equals(ClientErrorCode.CONNECTION_REFUSED)
                        || errorCode.equals(ClientErrorCode.UNKNOWN_HOST)
                        || errorCode.equals(ClientErrorCode.SOCKET_EXCEPTION)) {
                    return true;
                }

                // Don't retry when request input stream is non-repeatable
                if (errorCode.equals(ClientErrorCode.NONREPEATABLE_REQUEST)) {
                    return false;
                }
            }

            if (ex instanceof OSSException) {
                String errorCode = ((OSSException) ex).getErrorCode();
                // No need retry for invalid responses
                if (errorCode.equals(OSSErrorCode.INVALID_RESPONSE)) {
                    return false;
                }
            }

            if (response != null) {
                int statusCode = response.getStatusCode();
                if (statusCode == HTTP_INTERNAL_ERROR
                        || statusCode == HTTP_UNAVAILABLE) {
                    return true;
                }
            }

            return false;
        }
    }

    @Override
    protected RetryStrategy getDefaultRetryStrategy() {
        return new DefaultRetryStrategy();
    }

    @Override
    public void shutdown() {
        try {
            if (httpClient.dispatcher().executorService() != null) {
                httpClient.dispatcher().executorService().shutdown();
            }
            if (httpClient.connectionPool() != null) {
                httpClient.connectionPool().evictAll();
            }
            if (httpClient.cache() != null) {
                httpClient.cache().close();
            }
        } catch (IOException e) {
            logException("shutdown throw exception: ", e);
        }
    }
}
