package com.aliyun.oss.model;

import com.aliyun.oss.common.utils.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class ResponseBytes<T> {
    private final T response;

    private final byte[] bytes;

    public ResponseBytes(T response, byte[] bytes) {
        if (response == null || bytes == null)
            throw new NullPointerException("response must not be null.");
        if (bytes == null)
            throw new NullPointerException("bytes must not be null.");
        this.response = response;
        this.bytes = bytes;
    }

    public T response() {
        return this.response;
    }

    public final ByteBuffer asByteBuffer() {
        return ByteBuffer.wrap(this.bytes).asReadOnlyBuffer();
    }

    public final String asString(Charset charset) throws CharacterCodingException {
        return StringUtils.fromBytes(this.bytes, charset);
    }

    public final String asUtf8String() throws CharacterCodingException {
        return this.asString(UTF_8);
    }

    public final InputStream asInputStream() {
        return new ByteArrayInputStream(this.bytes);
    }
}
