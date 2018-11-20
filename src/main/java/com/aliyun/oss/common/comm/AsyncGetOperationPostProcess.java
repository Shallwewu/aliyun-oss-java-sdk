package com.aliyun.oss.common.comm;

import com.aliyun.oss.common.utils.CRC64;
import com.aliyun.oss.model.OSSObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.CheckedInputStream;

public class AsyncGetOperationPostProcess implements AsyncPostProcess<OSSObject> {

    @Override
    public void postProcess(OSSObject ossObject, long begin) throws IOException {
        InputStream instream = ossObject.getObjectContent();
        CRC64 crc = new CRC64();
        CheckedInputStream checkedInputstream = new CheckedInputStream(instream, crc);
        ossObject.setObjectContent(checkedInputstream);
        ossObject.close();
    }

}
