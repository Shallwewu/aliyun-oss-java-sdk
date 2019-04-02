package com.aliyun.oss.common.comm.async;

import com.aliyun.oss.common.utils.CRC64;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ResponseBytes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.CheckedInputStream;

public class AsyncGetOperationPostProcess extends AsyncPostProcess<OSSObject> {

    @Override
    public void postProcess(OSSObject ossObject, CallbackImpl callback) {
        InputStream instream = ossObject.getObjectContent();
        CRC64 crc = new CRC64();
        CheckedInputStream checkedInputstream = new CheckedInputStream(instream, crc);
        ossObject.setObjectContent(checkedInputstream);
        callback.setWrappedResult(ossObject);
    }

}
