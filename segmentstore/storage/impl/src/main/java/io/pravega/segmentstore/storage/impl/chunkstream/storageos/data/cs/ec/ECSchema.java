package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ec;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni.CRC;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni.EC;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.rpc.cm.CmMessage;

public class ECSchema {
    public final int DataNumber;
    public final int CodeNumber;
    public final int SegmentLength;
    public final long ChecksumOfPaddingSegment;
    public final long gfTable;
    public final ECCodeNettyMatrixCache codeMatrixCache;
    public final ECDataNativeSegmentCache dataSegmentCache;
    public final CmMessage.EcCodeScheme ecCodeScheme;

    public ECSchema(int dataNumber, int codeNumber, int segmentLength, CSConfiguration csConfig, int maxCodeMatrixCache, int maxDataSegmentCache) {
        ecCodeScheme = CmMessage.EcCodeScheme.newBuilder().setNumberOfDataBlocks(dataNumber).setNumberOfCodeBlocks(codeNumber).build();
        DataNumber = dataNumber;
        CodeNumber = codeNumber;
        SegmentLength = segmentLength;
        ChecksumOfPaddingSegment = CRC.crc32OfPadding((byte) 0, this.SegmentLength);
        gfTable = EC.make_encode_gf_table(DataNumber, CodeNumber);
        codeMatrixCache = new ECCodeNettyMatrixCache(this, csConfig.initCodeMatrixCache(), maxCodeMatrixCache);
        dataSegmentCache = new ECDataNativeSegmentCache(this, csConfig.initDataSegmentCache(), maxDataSegmentCache);
    }

    public long codeSegmentAddress(long matrix, int codeIndex) {
        return codeMatrixCache.segmentOfBuffer(matrix, codeIndex);
    }

    public void shutdown() {
        codeMatrixCache.destroyBuffers();
        dataSegmentCache.destroyBuffers();
    }

    @Override
    public String toString() {
        return "ECSchema{" +
               "DataNumber=" + DataNumber +
               ", CodeNumber=" + CodeNumber +
               ", SegmentLength=" + SegmentLength +
               '}';
    }
}
