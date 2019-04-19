package com.aliyun.oss.common.comm.async;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.common.utils.CRC64;
import com.aliyun.oss.common.utils.IOUtils;
import com.aliyun.oss.internal.OSSHeaders;
import com.aliyun.oss.internal.OSSUtils;
import com.aliyun.oss.model.OSSObject;

import java.io.*;
import java.util.zip.CheckedInputStream;

import static com.aliyun.oss.common.utils.IOUtils.safeClose;
import static com.aliyun.oss.common.utils.LogUtils.logException;
import static com.aliyun.oss.internal.OSSConstants.DEFAULT_BUFFER_SIZE;
import static com.aliyun.oss.internal.OSSUtils.OSS_RESOURCE_MANAGER;

public class AsyncGetOperationToFilePostProcess extends AsyncPostProcess<OSSObject> {

    private File file;

    public AsyncGetOperationToFilePostProcess(File file) {
        this.file = file;
    }

    @Override
    public void postProcess(OSSObject ossObject, CallbackImpl callback) {
        InputStream instream = ossObject.getObjectContent();
        CRC64 crc = new CRC64();
        CheckedInputStream checkedInputstream = new CheckedInputStream(instream, crc);
        ossObject.setObjectContent(checkedInputstream);

        OutputStream outputStream = null;
        try {
            outputStream = new BufferedOutputStream(new FileOutputStream(file));
            byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = ossObject.getObjectContent().read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }

            if (callback.getClientConfiguration().isCrcCheckEnabled() && callback.getRequest().getHeaders().get(OSSHeaders.RANGE) != null) {
                Long clientCRC = IOUtils.getCRCValue(ossObject.getObjectContent());
                OSSUtils.checkChecksum(clientCRC, ossObject.getServerCRC(), ossObject.getRequestId());
            }

            callback.setWrappedResult(ossObject.getObjectMetadata());
        } catch (IOException ex) {
            logException("[Post Process]:Cannot read object content stream: ", ex);
            callback.setException(new ClientException(OSS_RESOURCE_MANAGER.getString("CannotReadContentStream"), ex));
        } finally {
            safeClose(outputStream);
            safeClose(ossObject.getObjectContent());
        }
    }
}
