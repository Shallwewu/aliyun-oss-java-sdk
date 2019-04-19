package com.aliyun.oss.common.comm.async;

import com.aliyun.oss.*;
import com.aliyun.oss.common.comm.*;
import com.aliyun.oss.common.parser.ResponseParseException;
import com.aliyun.oss.common.parser.ResponseParser;
import com.aliyun.oss.common.utils.ExceptionFactory;
import com.aliyun.oss.internal.OSSUtils;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.Response;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.aliyun.oss.common.utils.LogUtils.logException;
import static com.aliyun.oss.internal.OSSUtils.COMMON_RESOURCE_MANAGER;

public class CallbackImpl<T, RESULT> implements Callback {

    private RequestMessage requestMessage;

    private ServiceClient.Request request;

    private ClientConfiguration clientConfiguration;

    private ExecutionContext context;

    private ResponseParser<T> parser;

    private boolean keepResponseOpen;

    private Exception exception;

    private AsyncPostProcess postProcess;

    private AsyncHandler<RESULT> asyncHandler;

    private CountDownLatch latch = new CountDownLatch(1);

    private RESULT wrappedResult;

    public RequestMessage getRequestMessage() {
        return requestMessage;
    }

    public void setRequestMessage(RequestMessage requestMessage) {
        this.requestMessage = requestMessage;
    }

    public ServiceClient.Request getRequest() {
        return request;
    }

    public ClientConfiguration getClientConfiguration() {
        return clientConfiguration;
    }

    public void setClientConfiguration(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = clientConfiguration;
    }

    public void setRequest(ServiceClient.Request request) {
        this.request = request;
    }

    public void setContext(ExecutionContext context) {
        this.context = context;
    }

    public void setParser(ResponseParser<T> parser) {
        this.parser = parser;
    }

    public void setKeepResponseOpen(boolean keepResponseOpen) {
        this.keepResponseOpen = keepResponseOpen;
    }

    public void setPostProcess(AsyncPostProcess postProcess) {
        this.postProcess = postProcess;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    public void setAsyncHandler(AsyncHandler<RESULT> asyncHandler) {
        this.asyncHandler = asyncHandler;
    }

    public RESULT getWrappedResult() {
        return wrappedResult;
    }

    public void setWrappedResult(RESULT wrappedResult) {
        this.wrappedResult = wrappedResult;
    }

    private void asyncCallback() {
        try {
            if (asyncHandler != null) {
                if (exception != null) {
                    asyncHandler.onError(exception);
                } else {
                    asyncHandler.onSuccess(wrappedResult);
                }
            }
        } catch (Exception ex) {
            logException("[Client]user async call back exception: ", ex,
                    requestMessage.getOriginalRequest().isLogEnabled());
        }
    }

    @Override
    public void onFailure(Call call, IOException e) {
        try {
            exception = ExceptionFactory.createNetworkException(e);
            asyncCallback();
        } finally {
            latch.countDown();
        }
    }

    @Override
    public void onResponse(Call call, Response response) {
        ResponseMessage responseMessage = null;

        try {
            responseMessage = DefaultServiceClient.buildResponse(request, response);

            handleResponse(responseMessage, context.getResponseHandlers());

            T parseResult = parser.parse(responseMessage);

            postProcess.postProcess(parseResult, this);
        } catch (ServiceException sex) {
            logException("[Server]Unable to execute HTTP request: ", sex,
                    requestMessage.getOriginalRequest().isLogEnabled());
            assert (sex instanceof OSSException);
            exception = sex;
        } catch (ClientException cex) {
            logException("[Client]Unable to execute HTTP request: ", cex,
                    requestMessage.getOriginalRequest().isLogEnabled());
            exception = cex;
        } catch (ResponseParseException rpe) {
            exception = ExceptionFactory.createInvalidResponseException(responseMessage.getRequestId(), rpe.getMessage(), rpe);
            logException("Unable to parse response error: ", rpe);
        } catch (Exception ex) {
            logException("[Unknown]Unable to execute HTTP request: ", ex,
                    requestMessage.getOriginalRequest().isLogEnabled());
            exception = new ClientException(
                    COMMON_RESOURCE_MANAGER.getFormattedString("ConnectionError", ex.getMessage()), ex);
        } finally {
            try {
                request.close();

                if (responseMessage != null && !keepResponseOpen) {
                    OSSUtils.safeCloseResponse(responseMessage);
                }
            } catch (IOException iex) {
                logException("Unexpected io exception when trying to close http request: ", iex);
            }
            asyncCallback();
            latch.countDown();
        }
    }

    private void handleResponse(ResponseMessage response, List<ResponseHandler> responseHandlers)
            throws ServiceException, ClientException {
        for (ResponseHandler h : responseHandlers) {
            h.handle(response);
        }
    }
}
