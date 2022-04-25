package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.lister;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.bucket.Bucket;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.*;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.CmClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.DTType;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt.DTRecords;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.WritePendingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

public class AsyncChunkObjectGreedyLister extends AsyncChunkObjectsLister {
    private static final Logger log = LoggerFactory.getLogger(AsyncChunkObjectGreedyLister.class);
    private final List<DTRecords.DirectoryKVEntry> entryList;
    private int remainingKeys;

    public AsyncChunkObjectGreedyLister(HttpServletResponse resp,
                                        AsyncContext asyncContext,
                                        Bucket bucket,
                                        String token,
                                        int maxKeys,
                                        ServletOutputStream out,
                                        ForkJoinPool executor,
                                        Cluster cluster,
                                        CmClient cmClient,
                                        boolean v2,
                                        String prefix) {
        super(asyncContext, bucket, token, cluster, cmClient, maxKeys, out, executor, v2, resp, prefix);
        remainingKeys = maxKeys;
        entryList = new ArrayList<DTRecords.DirectoryKVEntry>(maxKeys);
        parseToken(bucket, token);
    }

    @Override
    protected void listChunks() throws CSException, IOException {
        var chunkprefix = ChunkObjectKeyGenerator.chunkPrefixFromPrefix(bucket, prefix);
        cmClient.listChunks(ctIndex, startChunkId, endChunkId, remainingKeys, chunkprefix).whenComplete((r, t) -> {
            if (t != null) {
                log.error("r-{} list bucket {} ct {} range [{}, {}) failed", requestId, bucket.name, ctIndex, startChunkId, endChunkId, t);
                var msg = "r-" + requestId + " list bucket " + bucket.name + " ct " + ctIndex + " range [" + startChunkId + ", " + endChunkId + ") failed";
                try {
                    S3ErrorMessage.makeS3ErrorResponse(resp, S3ErrorCode.InternalError, null, msg);
                } catch (IOException e) {
                    log.error("list chunk send response with error message hit IOException, request id{ }, chunkid {}", requestId);
                    S3ErrorMessage.handleIOException(resp);
                }
                asyncContext.complete();
                return;
            }
            try {
                String nextToken;
                boolean truncated;
                if (r.getRight() != null) {
                    truncated = true;
                    nextToken = SchemaUtils.makeListToken(ctIndex, r.getRight());
                } else if (ctIndex < cluster.dtNumber(DTType.CT) - 1) {
                    ++ctIndex;
                    truncated = true;
                    nextToken = SchemaUtils.makeListToken(ctIndex, ChunkObjectId.makeChunkObjectIdStartKey(bucket.index));
                } else {
                    truncated = false;
                    nextToken = null;
                }
                if (!r.getLeft().isEmpty()) {
                    entryList.addAll(r.getLeft());
                    remainingKeys -= r.getLeft().size();
                }
                if (remainingKeys > 0 && truncated) {
                    parseToken(bucket, nextToken);
                    listChunks();
                } else {
                    var listResult = v2 ? composeV2ListResult(entryList, nextToken, truncated) : composeV1ListResult(entryList, nextToken, truncated);

                    flushing.setOpaque(true);
                    if (out.isReady()) {
                        resp.setStatus(HttpServletResponse.SC_OK);
                        out.print(listResult);
                        if (out.isReady()) {
                            try {
                                out.flush();
                                asyncContext.complete();
                            } catch (WritePendingException e) {
                                log.warn("r-{} flush list bucket {} ct {} range [{}, {}) failed, continue flush in onWritePossible", requestId, bucket.name, ctIndex, startChunkId, endChunkId, e);
                            }
                        } else {
                            log.warn("r-{} flush list bucket {} ct {} range [{}, {}) failed due to output is not ready, continue flush in onWritePossible", requestId, bucket.name, ctIndex, startChunkId, endChunkId);
                        }
                    } else {
                        pendingBuffer = listResult;
                        log.warn("r-{} write list bucket {} ct {} range [{}, {}) failed due to output is not ready, continue write in onWritePossible", requestId, bucket.name, ctIndex, startChunkId, endChunkId);
                    }
                }
            } catch (Throwable e) {
                log.error("r-{} list bucket {} ct {} range [{}, {}) send response failed", requestId, bucket.name, ctIndex, startChunkId, endChunkId, e);
                var msg = "r-" + requestId + " list bucket " + bucket.name + " ct " + ctIndex + " range [" + startChunkId + ", " + endChunkId + ") send response failed";
                try {
                    S3ErrorMessage.makeS3ErrorResponse(resp, S3ErrorCode.InternalError, null, msg);
                } catch (IOException ex) {
                    log().error("list bucket {} send response with error message hit IOException, request id {}", bucket.name, requestId);
                    S3ErrorMessage.handleIOException(resp);
                }
                asyncContext.complete();
            }
        });
    }

    @Override
    protected Logger log() {
        return log;
    }
}
