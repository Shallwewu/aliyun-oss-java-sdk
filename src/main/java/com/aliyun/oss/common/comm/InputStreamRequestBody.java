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

public class InputStreamRequestBody extends RequestBody {

    private InputStream content;

    private MediaType mediaType;

    private long contentLength;

    public InputStreamRequestBody(ServiceClient.Request request) {
        InputStream content = request.getContent();

        if (content == null) {
            throw new IllegalArgumentException("Source input stream may not be null");
        }
        this.content = content;
        this.contentLength = request.getContentLength();

        String contentType = request.getHeaders().get(HttpHeaders.CONTENT_TYPE);

        if (contentType != null) {
            mediaType = MediaType.parse(contentType);

//            if (mediaType != null) {
//                mediaType.charset(Charset.forName(OSSConstants.DEFAULT_CHARSET_NAME));
//            }
        }
    }

    @Override
    public MediaType contentType() {
        return mediaType;
    }

    @Override
    public long contentLength() throws IOException {
        return contentLength;
    }

    @Override
    public void writeTo(BufferedSink bufferedSink) throws IOException {
        Source source = null;

        try {
            source = Okio.source(content);
            bufferedSink.writeAll(source);
        } finally {
            Util.closeQuietly(source);
        }
    }
}
