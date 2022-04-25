package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.servlet.stream;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.*;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Count;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.metric.Metrics;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.FutureHeapWriteDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.SingleWriteBatchFeatureHeapWriteDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.obj.SharedObjWriter;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.etag.ETagHandler;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static jakarta.servlet.http.HttpServletResponse.SC_OK;

public abstract class SharedObjAsyncStreamReader extends AsyncStreamReader implements InputDataObjFetcher {
    private static final Count onDataAvailableCount = Metrics.makeMetric("SharedObj.onDataAvailable.count", Count.class);
    protected final SharedObjWriter sharedObjWriter;
    protected final ETagHandler eTagHandler;
    protected final long startNano;
    private final LocationCoder.CodeType locationType;

    public SharedObjAsyncStreamReader(String objKey,
                                      CSServletInputStream inputStream,
                                      long contentLength,
                                      int indexGranularity,
                                      AsyncContext asyncContext,
                                      SharedObjWriter sharedObjWriter,
                                      LocationCoder.CodeType locationType,
                                      CSConfiguration csConfig,
                                      long startNs) {
        super(objKey, inputStream, contentLength, indexGranularity, asyncContext, csConfig);
        this.sharedObjWriter = sharedObjWriter;
        this.locationType = locationType;
        // cj_todo remove second parameter after support native memory on shared object
        this.eTagHandler = ETagHandler.createETagHandler(csConfig, true);
        this.startNano = startNs;
    }

    @Override
    public void onDataAvailable() throws IOException {
        onDataAvailableCount.increase();
        read();
    }

    @Override
    public void onAllDataRead() throws IOException {
        eTagHandler.end();
        var timeoutSeconds = TimeoutUtil.asyncWriteDiskTimeoutSeconds(contentLength, csConfig);
        CompletableFuture.allOf(makeWaitFutures())
                         .orTimeout(timeoutSeconds, TimeUnit.SECONDS)
                         .whenComplete((r, t) -> {
                             var resp = (HttpServletResponse) asyncContext.getResponse();
                             try {
                                 if (t != null) {
                                     if (t instanceof TimeoutException) {
                                         log().error("r-{} write obj {} failed with timeout {}s", requestId, objKey, timeoutSeconds, t);
                                         var msg = "r-" + requestId + " write obj " + objKey + " failed with timeout " + timeoutSeconds + "s";
                                         S3ErrorMessage.makeS3ErrorResponse(resp, S3ErrorCode.RequestTimeout, objKey, msg);
                                     } else {
                                         log().error("r-{} write obj {} failed", requestId, objKey, t);
                                         var msg = "r-" + requestId + " write obj " + objKey + " failed";
                                         S3ErrorMessage.makeS3ErrorResponse(resp, S3ErrorCode.InternalError, objKey, msg);
                                     }
                                     return;
                                 }
                                 var contentMD5 = ((HttpServletRequest) asyncContext.getRequest()).getHeader("Content-MD5");
                                 if (contentMD5 != null && !csConfig.skipMD5()) {
                                     var decode = Base64.getDecoder().decode(contentMD5);
                                     if (!Arrays.equals(decode, eTagHandler.md5Bytes())) {
                                         log().error("r-{} write obj {} header Content-MD5 {} != receive MD5 {}",
                                                     requestId, objKey, contentMD5, eTagHandler.md5Base64());
                                         var msg = "r-" + requestId + " write obj " + objKey + " header Content-MD5 " + contentMD5 + " != receive MD5 " + eTagHandler.md5Base64();
                                         S3ErrorMessage.makeS3ErrorResponse(resp, S3ErrorCode.BadDigest, objKey, msg);
                                         return;
                                     }
                                 } else {
                                     log().debug("r-{} skip header Content-MD5 check for obj {} receive MD5 {}",
                                                 requestId, objKey, eTagHandler.md5Base64());
                                 }
                                 if (!csConfig.skipMD5()) {
                                     resp.addHeader("ETag", eTagHandler.eTagHex());
                                 }
                                 LocationCoder.encode(resp.getOutputStream(), getWriteLocations(), locationType);
                                 resp.setStatus(SC_OK);
                                 onSuccess();
                             } catch (Exception e) {
                                 log().error("r-{} failed write obj {} during wait and write result",
                                             requestId, objKey, e);
                                 var msg = "r-" + requestId + " failed write obj " + objKey + " during wait and write result";
                                 try {
                                     S3ErrorMessage.makeS3ErrorResponse(resp, S3ErrorCode.InternalError, objKey, msg);
                                 } catch (IOException ex) {
                                     log().error("write obj send response with error message hit IOException, objKey {}, request id  {}", objKey, requestId);
                                     S3ErrorMessage.handleIOException(resp);
                                 }
                             } finally {
                                 asyncContext.complete();
                             }
                         });
    }

    @Override
    public void onError(Throwable t) {
        eTagHandler.cancel();
        eTagHandler.errorEnd();
        log().error("r-{} write obj {} failed", requestId, objKey, t);
        var resp = (HttpServletResponse) asyncContext.getResponse();
        var msg = "r-" + requestId + " write obj " + objKey + " failed";
        try {
            S3ErrorMessage.makeS3ErrorResponse(resp, S3ErrorCode.InternalError, objKey, msg);
        } catch (IOException ex) {
            log().error("write obj send response with error message hit IOException, objKey {}, request id  {}", objKey, requestId);
            S3ErrorMessage.handleIOException(resp);
        }
        asyncContext.complete();
    }

    /*
        InputDataObjFetcher
         */
    @Override
    public void onFetchObjData() {
        if (requireData.compareAndSet(false, true)) {
            read();
        }
    }

    @Override
    public String name() {
        return objKey;
    }

    @Override
    protected boolean requireExtraFetchData() {
        return sharedObjWriter.requireExtraFetchData();
    }

    @Override
    public int compareTo(InputDataObjFetcher o) {
        if (!(o instanceof SharedObjAsyncStreamReader)) {
            return -1;
        }
        if (this == o) {
            return 0;
        }
        return (int) (startNano - ((SharedObjAsyncStreamReader) o).startNano);
    }

    @Override
    protected WriteDataBuffer makeBuffer(long objStart) {
        if (leftLength > 0) {
            // cj_todo use buffer pool
            // directory buffer pool will better
            // use native mem pool will eliminate mem copy
            // use rdma registered native mem pool will eliminate all mem copy in write path
            var size = (int) Math.min(leftLength, indexGranularity);
            if (size >= CSConfiguration.writeSegmentSize()) {
                assert size == CSConfiguration.writeSegmentSize();
                return SingleWriteBatchFeatureHeapWriteDataBuffer.allocate(objStart, size, requestId, nextPartId());
            }
            return FutureHeapWriteDataBuffer.allocate(objStart, size, requestId, nextPartId());
        }
        return StringUtils.dumbBuffer;
    }

    protected abstract List<Location> getWriteLocations() throws ExecutionException, InterruptedException;

    protected abstract CompletableFuture<?>[] makeWaitFutures();

    protected abstract int nextPartId();

    protected abstract void onSuccess();
}
