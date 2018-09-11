package com.aliyun.oss.common.comm;

import com.aliyun.oss.*;
import com.aliyun.oss.common.utils.ExceptionFactory;
import com.aliyun.oss.common.utils.HttpUtil;
import com.aliyun.oss.common.utils.IOUtils;
import okhttp3.*;
import okhttp3.Protocol;
import org.apache.http.HttpStatus;

import javax.net.ssl.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.security.KeyManagementException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.aliyun.oss.common.utils.LogUtils.logException;

public class OKHTTPServiceClient extends ServiceClient {

    private static final int MAX_IDLE_CONNECTIONS = 5;

    private static final long READ_TIMEOUT = 10000;

    private static final long WRITE_TIMEOUT = 10000;

    protected static HttpRequestFactory httpRequestFactory = new HttpRequestFactory();

    protected OkHttpClient httpClient;

    public OKHTTPServiceClient(ClientConfiguration config) {
        super(config);
        ConnectionPool connectionPool = new ConnectionPool(MAX_IDLE_CONNECTIONS, config.getIdleConnectionTime(), TimeUnit.MILLISECONDS);

        OkHttpClient.Builder builder = new OkHttpClient.Builder().followRedirects(false).followSslRedirects(false)
                .connectTimeout(config.getConnectionTimeout(), TimeUnit.MILLISECONDS).readTimeout(READ_TIMEOUT, TimeUnit.MILLISECONDS)
                .writeTimeout(WRITE_TIMEOUT, TimeUnit.MILLISECONDS).retryOnConnectionFailure(false).connectionPool(connectionPool);

        List<okhttp3.Protocol> protocols = new ArrayList<okhttp3.Protocol>();
        protocols.add(okhttp3.Protocol.HTTP_2);
        protocols.add(okhttp3.Protocol.HTTP_1_1);
//        protocols.add(Protocol.H2_PRIOR_KNOWLEDGE);

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
    }

    @Override
    protected ResponseMessage sendRequestCore(Request request, ExecutionContext context) throws IOException {
        okhttp3.Request.Builder builder = httpRequestFactory.createHttpRequestBuilder(request, context);

        okhttp3.Request httpRequest = builder.build();

        Response response;
        Call call = httpClient.newCall(httpRequest);
        try {
            response = call.execute();
        } catch (IOException ex) {
            call.cancel();  //necessary?
            throw ExceptionFactory.createNetworkException(ex);
        }

        return buildResponse(request, response);
    }

    protected static ResponseMessage buildResponse(ServiceClient.Request request, Response httpResponse)
            throws IOException {

        assert (httpResponse != null);

        ResponseMessage response = new ResponseMessage(request);
        response.setUrl(request.getUri());
        response.setResponse(httpResponse);

        response.setStatusCode(httpResponse.code());

        if (httpResponse.body() != null) {
            if (response.isSuccessful()) {
                response.setContent(httpResponse.body().byteStream());
            } else {
                readAndSetErrorResponse(httpResponse.body().byteStream(), response);
            }
        }

        for (int index = 0; index < httpResponse.headers().size(); index++) {
            response.addHeader(httpResponse.headers().name(index), httpResponse.headers().value(index));
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
                if (statusCode == HttpStatus.SC_INTERNAL_SERVER_ERROR
                        || statusCode == HttpStatus.SC_SERVICE_UNAVAILABLE) {
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
