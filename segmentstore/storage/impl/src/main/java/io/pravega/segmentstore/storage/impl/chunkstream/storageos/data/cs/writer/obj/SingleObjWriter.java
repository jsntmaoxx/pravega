package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.obj;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.chunk.ChunkObject;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.*;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.WriteChunkHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.WriteType1ChunkHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.WriteType2ChunkHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.pipe.etag.ETagHandler;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.servlet.stream.InputDataChunkFetcher;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static jakarta.servlet.http.HttpServletResponse.SC_OK;

public abstract class SingleObjWriter implements ObjectWriter {
    protected final String objKey;
    protected final InputDataChunkFetcher inputDataFetcher;
    protected final ETagHandler eTagHandler;
    protected final long startNano;
    protected final CSConfiguration csConfig;
    private final LocationCoder.CodeType locationType;
    private final long timeoutSeconds;

    public SingleObjWriter(String objKey,
                           InputDataChunkFetcher inputDataFetcher,
                           LocationCoder.CodeType locationType,
                           CSConfiguration csConfig,
                           long startNs,
                           long timeoutSeconds) {
        this.objKey = objKey;
        this.inputDataFetcher = inputDataFetcher;
        this.locationType = locationType;
        this.csConfig = csConfig;
        this.timeoutSeconds = timeoutSeconds;
        this.eTagHandler = ETagHandler.createETagHandler(csConfig, false);
        this.startNano = startNs;
    }

    @Override
    public void onFinishReadData(AsyncContext asyncContext) {
        activeWriteChunkHandler().end();
        eTagHandler.end();
        // cj_todo if write error happens before finish read all data,
        // the error can not return to here.
        CompletableFuture.allOf(makeWaitFutures())
                         .orTimeout(timeoutSeconds, TimeUnit.SECONDS)
                         .whenComplete((r, t) -> {
                             var resp = (HttpServletResponse) asyncContext.getResponse();
                             try {
                                 if (t != null) {
                                     log().error("write obj {} failed", objKey, t);
                                     var msg = "write obj " + objKey + "failed";
                                     try {
                                         S3ErrorMessage.makeS3ErrorResponse(resp, S3ErrorCode.InternalError, objKey, msg);
                                     } catch (IOException ex) {
                                         log().error("write obj send response with error message hit IOException, objKey {}", objKey);
                                         S3ErrorMessage.handleIOException(resp);
                                     }
                                     return;
                                 }
                                 var contentMD5 = ((HttpServletRequest) asyncContext.getRequest()).getHeader("Content-MD5");
                                 if (contentMD5 != null && !csConfig.skipMD5()) {
                                     var decode = Base64.getDecoder().decode(contentMD5);
                                     if (!Arrays.equals(decode, eTagHandler.md5Bytes())) {
                                         log().error("write obj {} header Content-MD5 {} != receive MD5 {}",
                                                     objKey, contentMD5, eTagHandler.md5Base64());
                                         var msg = "write obj " + objKey + " header Content-MD5 " + contentMD5 + " != receive MD5 " + eTagHandler.md5Base64();
                                         try {
                                             S3ErrorMessage.makeS3ErrorResponse(resp, S3ErrorCode.BadDigest, objKey, msg);
                                         } catch (IOException ex) {
                                             log().error("write obj send response with error message hit IOException, objKey {}", objKey);
                                             S3ErrorMessage.handleIOException(resp);
                                         }
                                         return;
                                     }
                                 } else {
                                     log().info("skip header Content-MD5 check for obj {} receive MD5 {}", objKey, eTagHandler.md5Base64());
                                 }

                                 var locations = getWriteLocations();
                                 if (!csConfig.skipMD5()) {
                                     resp.addHeader("ETag", eTagHandler.eTagHex());
                                 }
                                 //LocationCoder.encode(resp.getOutputStream(), locations, locationType);
                                 resp.setStatus(SC_OK);
                                 log().info("write obj {} success - {}", objKey, LocationCoder.encode(locations, LocationCoder.CodeType.Plain));
                                 onSuccess();
                             } catch (Exception e) {
                                 log().error("write obj {} response failed", objKey, e);
                             } finally {
                                 asyncContext.complete();
                             }
                         });
    }

    @Override
    public void onErrorReadData(AsyncContext asyncContext, Throwable t) {
        log().error("write obj {} failed, cancel and end request with error", objKey, t);
        errorEndWriteChunkHandlers();
        eTagHandler.cancel();
        eTagHandler.errorEnd();
        var ecFuture = activeWriteChunkHandler().writeChunkFuture();
        var eTagFuture = eTagHandler.eTagFuture();
        CompletableFuture.allOf(eTagFuture, ecFuture)
                         .whenComplete((r, tx) -> {
                             var resp = (HttpServletResponse) asyncContext.getResponse();
                             var msg = "write obj " + objKey + " failed, cancel and end request with error";
                             try {
                                 S3ErrorMessage.makeS3ErrorResponse(resp, S3ErrorCode.InternalError, objKey, msg);
                             } catch (IOException ex) {
                                 log().error("write obj send response with error message hit IOException, objKey {}", objKey);
                                 S3ErrorMessage.handleIOException(resp);
                             }
                             asyncContext.complete();
                         });
    }

    protected WriteChunkHandler createWriteChunkHandler(ChunkObject chunkObj, ChunkConfig chunkConfig, Executor executor) {
        var ecSchema = chunkConfig.ecSchema();
        Long codeMatrix = null, dataSegment = null;
        try {
            codeMatrix = ecSchema.codeMatrixCache.borrow();
            dataSegment = ecSchema.dataSegmentCache.borrow();
            if (codeMatrix != null && dataSegment != null) {
                return new WriteType2ChunkHandler(chunkObj, codeMatrix, dataSegment, this.inputDataFetcher, executor, chunkConfig, chunkConfig.csConfig.maxPrepareWriteBatchCountInChunk());
            }
        } catch (Exception e) {
            log().error("write obj {} create type2 chunk handler failed", objKey, e);
        }
        ecSchema.codeMatrixCache.giveBack(codeMatrix);
        ecSchema.dataSegmentCache.giveBack(dataSegment);
        return new WriteType1ChunkHandler(chunkObj, this.inputDataFetcher, executor, null, null, chunkConfig);
    }

    protected abstract WriteChunkHandler activeWriteChunkHandler();

    /*
    valid only after all feature is success
     */
    public abstract List<Location> getWriteLocations();

    protected abstract CompletableFuture<?>[] makeWaitFutures();

    protected abstract void errorEndWriteChunkHandlers();

    protected abstract void onSuccess();
}
