package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.lister;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.bucket.Bucket;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.*;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.CmClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt.DTType;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.cm.CmMessage;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.dt.DTRecords;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.types.SchemaKeyRecords;
import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.channels.WritePendingException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AsyncChunkObjectsLister implements WriteListener {
    protected final AsyncContext asyncContext;
    protected final Bucket bucket;
    protected final String token;
    protected final Cluster cluster;
    protected final CmClient cmClient;
    protected final int maxKeys;
    protected final ServletOutputStream out;
    protected final ForkJoinPool executor;
    protected final UUID endChunkId;
    protected final boolean v2;
    protected final long requestId;
    protected final AtomicBoolean flushing = new AtomicBoolean(false);
    protected final HttpServletResponse resp;
    protected final String prefix;
    protected int ctIndex;
    protected UUID startChunkId;
    protected String pendingBuffer;
    protected SchemaKeyRecords.SchemaKey ctToken;

    public AsyncChunkObjectsLister(AsyncContext asyncContext, Bucket bucket, String token, Cluster cluster, CmClient cmClient, int maxKeys, ServletOutputStream out, ForkJoinPool executor, boolean v2, HttpServletResponse resp, String prefix) {
        this.asyncContext = asyncContext;
        this.bucket = bucket;
        this.token = token;
        this.cluster = cluster;
        this.cmClient = cmClient;
        this.maxKeys = maxKeys;
        this.out = out;
        this.executor = executor;
        this.endChunkId = ChunkObjectId.makeChunkObjectIdEndKey(bucket.index);
        this.v2 = v2;
        this.requestId = G.genRequestId();
        this.resp = resp;
        this.prefix = prefix;
    }

    @Override
    public void onWritePossible() throws IOException {
        try {
            if (flushing.getOpaque()) {
                if (!out.isReady()) {
                    log().warn("r-{} write and flush list bucket {} ct {} range [{}, {}) failed due to output is not ready, continue write flush next time", requestId, bucket.name, ctIndex, startChunkId, endChunkId);
                    return;
                }
                if (pendingBuffer != null) {
                    out.print(pendingBuffer);
                    log().warn("r-{} write list bucket {} ct {} range [{}, {}) in onWritePossible", requestId, bucket.name, ctIndex, startChunkId, endChunkId);
                    pendingBuffer = null;
                }
                try {
                    out.flush();
                    asyncContext.complete();
                    flushing.setOpaque(false);
                    log().warn("r-{} flush list bucket {} ct {} range [{}, {}) in onWritePossible success", requestId, bucket.name, ctIndex, startChunkId, endChunkId);
                } catch (WritePendingException e) {
                    log().warn("r-{} flush list bucket {} ct {} range [{}, {}) in onWritePossible failed, continue flush next time", requestId, bucket.name, ctIndex, startChunkId, endChunkId);
                }
                return;
            }
            listChunks();
        } catch (Exception e) {
            onError(e);
        }
    }

    protected String composeV1ListResult(ImmutablePair<List<DTRecords.DirectoryKVEntry>, SchemaKeyRecords.SchemaKey> result, String nextToken, boolean truncated) {
        return composeV1ListResult(result.getLeft(), nextToken, truncated);
    }

    protected String composeV1ListResult(List<DTRecords.DirectoryKVEntry> entryList, String nextToken, boolean truncated) {
        StringBuilder os = new StringBuilder().append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
                                              .append("<ListBucketResult>\n");
        os.append("\t<IsTruncated>")
          .append(truncated)
          .append("</IsTruncated>\n");
        if (token != null) {
            os.append("\t<Marker>")
              .append(token)
              .append("</Marker>\n");
        }
        if (nextToken != null) {
            os.append("\t<NextMarker>")
              .append(nextToken)
              .append("</NextMarker>\n");
        }
        for (var k : entryList) {
            try {
                var key = SchemaKeyRecords.ChunkKey.parseFrom(k.getSchemaKey().getUserKey());
                var chunk = key.hasChunkIdUuid()
                            ? new UUID(key.getChunkIdUuid().getHighBytes(), key.getChunkIdUuid().getLowBytes()).toString()
                            : key.getChunkId();
                var info = CmMessage.ChunkInfo.parseFrom(k.getValue());
                if (info.hasOriginalKey()) {
                    chunk = info.getOriginalKey();
                }
                if (info.getStatus() == CmMessage.ChunkStatus.SEALED) {
                    os.append("\t<Contents>\n")
                      .append("\t\t<Key>").append(chunk).append("</Key>\n")
                      .append("\t\t<Size>").append(info.getCapacity()).append("</Size>\n")
                      .append("\t\t<IndexGranularity>").append(info.getSealedLength()).append("</IndexGranularity>\n")
                      .append("\t</Contents>\n");
                }
            } catch (InvalidProtocolBufferException e) {
                log().error("r-{} parse chunk key {} failed", requestId, k.getSchemaKey().getUserKey().toStringUtf8());
            }
        }
        os.append("\t<Name>")
          .append(bucket.name)
          .append("</Name>\n");
        os.append("\t<Prefix>")
          .append(prefix)
          .append("</Prefix>\n");
        os.append("\t<MaxKeys>")
          .append(maxKeys)
          .append("</MaxKeys>\n");
        os.append("</ListBucketResult>\n");
        return os.toString();
    }

    protected String composeV2ListResult(ImmutablePair<List<DTRecords.DirectoryKVEntry>, SchemaKeyRecords.SchemaKey> result, String nextToken, boolean truncated) {
        return composeV2ListResult(result.getLeft(), nextToken, truncated);
    }

    protected String composeV2ListResult(List<DTRecords.DirectoryKVEntry> entryList, String nextToken, boolean truncated) {
        StringBuilder os = new StringBuilder().append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
                                              .append("<ListBucketResult>\n");
        os.append("\t<Name>")
          .append(bucket.name)
          .append("</Name>\n");
        os.append("\t<Prefix></Prefix>\n");
        if (token != null) {
            os.append("\t<ContinuationToken>")
              .append(token)
              .append("</ContinuationToken>\n");
        }
        if (nextToken != null) {
            os.append("\t<NextContinuationToken>")
              .append(nextToken)
              .append("</NextContinuationToken>\n");
        }
        os.append("\t<KeyCount>")
          .append(entryList.size())
          .append("</KeyCount>\n");
        os.append("\t<MaxKeys>")
          .append(maxKeys)
          .append("</MaxKeys>\n");
        os.append("\t<IsTruncated>")
          .append(truncated)
          .append("</IsTruncated>\n");
        for (var k : entryList) {
            try {
                var key = SchemaKeyRecords.ChunkKey.parseFrom(k.getSchemaKey().getUserKey());
                var chunk = key.hasChunkIdUuid()
                            ? new UUID(key.getChunkIdUuid().getHighBytes(), key.getChunkIdUuid().getLowBytes()).toString()
                            : key.getChunkId();
                var info = CmMessage.ChunkInfo.parseFrom(k.getValue());
                if (info.hasOriginalKey()) {
                    chunk = info.getOriginalKey();
                }
                if (info.getStatus() == CmMessage.ChunkStatus.SEALED) {
                    os.append("\t<Contents>\n")
                      .append("\t\t<Key>").append(chunk).append("</Key>\n")
                      .append("\t\t<Size>").append(info.getSealedLength()).append("</Size>\n")
                      .append("\t\t<IndexGranularity>").append(info.getIndexGranularity()).append("</IndexGranularity>\n")
                      .append("\t</Contents>\n");
                }
            } catch (InvalidProtocolBufferException e) {
                log().error("r-{} parse chunk key {} failed", requestId, k.getSchemaKey().getUserKey().toStringUtf8());
            }
        }
        os.append("</ListBucketResult>\n");
        return os.toString();
    }

    protected void parseToken(Bucket bucket, String token) {
        if (token == null || token.isEmpty()) {
            ctIndex = 0;
            ctToken = null;
            startChunkId = ChunkObjectId.makeChunkObjectIdStartKey(bucket.index);
            return;
        }
        try {
            var listToken = SchemaUtils.parseListToken(token);
            ctIndex = listToken.getLeft();
            ctToken = listToken.getRight();
            var chunkKey = SchemaKeyRecords.ChunkKey.parseFrom(ctToken.getUserKey());
            startChunkId = chunkKey.hasChunkId() ? UUID.fromString(chunkKey.getChunkId()) : new UUID(chunkKey.getChunkIdUuid().getHighBytes(), chunkKey.getChunkIdUuid().getLowBytes());
            if (startChunkId.compareTo(endChunkId) >= 0 && ctIndex < cluster.dtNumber(DTType.CT) - 1) {
                ++ctIndex;
            }
        } catch (Exception e) {
            log().error("r-{} parse token {} failed, ignore input token", requestId, token, e);
            ctIndex = 0;
            startChunkId = ChunkObjectId.makeChunkObjectIdStartKey(bucket.index);
            ctToken = null;
        }
    }

    @Override
    public void onError(Throwable t) {
        log().error("r-{} list bucket {} ct {} range [{}, {}) receive error", requestId, bucket.name, ctIndex, startChunkId, endChunkId, t);
        var msg = "r-" + requestId + " list bucket " + bucket.name + " ct " + ctIndex + " range [" + startChunkId + ", " + endChunkId + ") receive error";
        try {
            S3ErrorMessage.makeS3ErrorResponse(resp, S3ErrorCode.InternalError, null, msg);
        } catch (IOException ex) {
            log().error("list bucket {} send response with error message hit IOException, request id {}", bucket.name, requestId);
            S3ErrorMessage.handleIOException(resp);
        }
        asyncContext.complete();
    }

    protected abstract void listChunks() throws CSException, IOException;

    protected abstract Logger log();
}
