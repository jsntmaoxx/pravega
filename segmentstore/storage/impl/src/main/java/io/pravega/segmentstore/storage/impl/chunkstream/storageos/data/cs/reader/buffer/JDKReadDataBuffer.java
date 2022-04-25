package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

abstract class JDKReadDataBuffer implements ReadDataBuffer {
    protected final ByteBuffer data;
    protected List<ByteBuffer> contentBuffers;
    protected ParseResultStatus parseResultStatus = null;

    protected JDKReadDataBuffer(ByteBuffer data) {
        this.data = data;
    }

    @Override
    public int readChunkSize() throws CSException {
        return data.capacity();
    }

    @Override
    public ByteBuffer chunkBuffer() {
        return data;
    }

    @Override
    public List<ByteBuffer> contentBuffers() {
        assert contentBuffers != null;
        return contentBuffers;
    }

    @Override
    public boolean parseAndVerifyData() {
        data.clear();
        var buffer = parseNext(data, true);
        if (buffer == null) {
            return false;
        }
        if (!data.hasRemaining()) {
            contentBuffers = Collections.singletonList(buffer);
            return true;
        }
        // cj_todo init capacity
        contentBuffers = new ArrayList<>();
        contentBuffers.add(buffer);
        do {
            buffer = parseNext(data, true);
            if (buffer == null) {
                return false;
            }
            contentBuffers.add(buffer);
        } while (data.hasRemaining());

        return true;
    }

    public int writableBytes() {
        return data.remaining();
    }

    protected abstract Logger log();

    protected ByteBuffer parseNext(ByteBuffer inputData, boolean isLastBuffer) {
        var startPos = inputData.position();

        if (inputData.remaining() < CSConfiguration.writeSegmentHeaderLen()) {
            if (isLastBuffer) {
                log().error("verify read data failed no enough buffer remain: {} < segment header len {}",
                            inputData.remaining(), CSConfiguration.writeSegmentHeaderLen());
            }
            parseResultStatus = ParseResultStatus.INCOMPLETE;
            return null;
        }

        var subContentLen = inputData.getInt();
        if (subContentLen < 0 || subContentLen > (inputData.remaining() - CSConfiguration.writeSegmentFooterLen())) {
            if (isLastBuffer) {
                log().error("verify read data failed dataLen {} > max content size {}",
                            subContentLen, (inputData.limit() - inputData.position() - CSConfiguration.writeSegmentFooterLen()));
            }
            inputData.position(inputData.position() - 4);//return back the size part for the next cross-segment analysis
            parseResultStatus = ParseResultStatus.INCOMPLETE;
            return null;
        }
        var crcLen = CSConfiguration.writeSegmentHeaderLen() + subContentLen;
        var retChecksum = computeCRC32(inputData, startPos, crcLen);
        inputData.position(startPos + crcLen);
        var checksum = inputData.getLong();
        if (retChecksum != checksum) {
            log().error("verify read data failed return checksum {} != disk checksum {}", retChecksum, checksum);
            parseResultStatus = ParseResultStatus.CHECKSUM_UNMATCHED;
            return null;
        }
        inputData.position(startPos + CSConfiguration.writeSegmentOverhead() + subContentLen);
        parseResultStatus = ParseResultStatus.SUCCESS;
        return inputData.duplicate()
                        .position(startPos + CSConfiguration.writeSegmentHeaderLen())
                        .limit(startPos + CSConfiguration.writeSegmentHeaderLen() + subContentLen);
    }

    protected abstract long computeCRC32(ByteBuffer inputData, int startPos, int size);

    protected abstract long computeCRC32(ByteBuffer inputData1, int startPos1, int size1, ByteBuffer inputData2, int startPos2, int size2);

    protected enum ParseResultStatus {
        SUCCESS,
        INCOMPLETE,
        CHECKSUM_UNMATCHED
    }
}
