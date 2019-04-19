package com.aliyun.oss.common.comm;

import com.aliyun.oss.common.utils.HttpHeaders;
import com.aliyun.oss.internal.OSSConstants;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import okhttp3.internal.Util;
import okio.BufferedSink;
import okio.Okio;
import okio.Source;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class RepeatableInputStreamEntity extends RequestBody {

    private boolean firstAttempt = true;

    private InputStream content;

    private MediaType mediaType;

    private long contentLength;

    public RepeatableInputStreamEntity(ServiceClient.Request request) {
        InputStream content = request.getContent();

        if (content == null) {
            throw new IllegalArgumentException("Source input stream may not be null");
        }
        this.content = content;
        this.contentLength = request.getContentLength();

        String contentType = request.getHeaders().get(HttpHeaders.CONTENT_TYPE);

        if (contentType != null) {
            mediaType = MediaType.parse(contentType);
        }
    }

    @Override
    public MediaType contentType() {
        return mediaType;
    }

    public boolean isRepeatable() {
        return true;
    }

    @Override
    public long contentLength() throws IOException {
        return contentLength;
    }

    @Override
    public void writeTo(BufferedSink bufferedSink) throws IOException {
        if (!firstAttempt && isRepeatable()) {
            content.reset();
        }

        firstAttempt = false;

        Source source = Okio.source(content);
        bufferedSink.writeAll(source);
        //not close input stream here
    }
}
