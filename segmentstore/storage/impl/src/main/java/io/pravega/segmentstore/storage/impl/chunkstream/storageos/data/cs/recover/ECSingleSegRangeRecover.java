package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.recover;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.G;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ec.ECSchema;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni.EC;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer.DirectReadDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer.ReadDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.cm.CmMessage;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskClient;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.disk.DiskMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class ECSingleSegRangeRecover extends AbstractECRecover {
    private static final Logger log = LoggerFactory.getLogger(ECSingleSegRangeRecover.class);

    private final int recoverySegIndex;
    private final int segOffset;
    private final int length;

    public ECSingleSegRangeRecover(String requestId,
                                   DiskClient<? extends DiskMessage> diskClient,
                                   UUID chunkId,
                                   ECSchema ecSchema,
                                   CmMessage.Copy copy,
                                   int recoverySegIndex,
                                   int segOffset,
                                   int length) {
        super(requestId, diskClient, chunkId, ecSchema, copy);
        this.recoverySegIndex = recoverySegIndex;
        this.segOffset = segOffset;
        this.length = length;
    }

    @Override
    public CompletableFuture<ReadDataBuffer> run() {
        var recoverFuture = new CompletableFuture<ReadDataBuffer>();
        var dataSet = prepareDataSet(segOffset, length, Collections.singletonList(recoverySegIndex));
        readAndRecover(recoverFuture, dataSet, ecSchema.DataNumber);
        return recoverFuture;
    }

    @Override
    protected void recover(CompletableFuture<ReadDataBuffer> recoverFuture, DataSet dataSet) {
        var dataNum = ecSchema.DataNumber;
        var codeNum = ecSchema.CodeNumber;
        var recoverNum = 1;
        long srcMatrix = 0, decodeMatrix = 0, decodeGfTable = 0;
        try {
            // prepare seg status and source data matrix
            var vSegState = new boolean[dataNum + codeNum];
            srcMatrix = EC.make_buffer(dataNum, Long.BYTES);
            long di = 0;
            for (var si = 0; si < dataSet.vReadSeg.length; ++si) {
                var rs = dataSet.vReadSeg[si];
                if (rs != null && rs.dataBuffer != null) {
                    vSegState[si] = true;
                    G.U.putLong(srcMatrix + (di * Long.BYTES), rs.dataBuffer.nativeDataAddress());
                    ++di;
                }
            }
            // prepare decode matrix as recover result
            var decodeBuffer = ByteBuffer.allocateDirect(length);
            decodeMatrix = EC.make_buffer(recoverNum, Long.BYTES);
            G.U.putLong(decodeMatrix, ((DirectBuffer) decodeBuffer).address());

            decodeGfTable = EC.make_decode_gf_table(dataNum, codeNum, vSegState, codeNum, recoverySegIndex);

            if (EC.encode(dataNum, recoverNum, length, decodeGfTable, srcMatrix, decodeMatrix)) {
                recoverFuture.complete(new DirectReadDataBuffer(decodeBuffer));
            } else {
                recoverFuture.completeExceptionally(new CSException("recover failed due to decode error"));
            }
        } catch (Throwable e) {
            log().error("{} recover chunk {} failed", requestId, chunkId, e);
        } finally {
            if (srcMatrix != 0) {
                EC.destroy_buffer(srcMatrix);
            }
            if (decodeMatrix != 0) {
                EC.destroy_buffer(decodeMatrix);
            }
            if (decodeGfTable != 0) {
                EC.destroy_buffer(decodeGfTable);
            }
        }
    }

    @Override
    protected Logger log() {
        return log;
    }
}
