package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSConfiguration;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSException;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.reader.buffer.JDKReadDataBuffer.ParseResultStatus;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TwoReadDataBuffers implements ReadDataBuffer {

    private static final Logger log = LoggerFactory.getLogger(TwoReadDataBuffers.class);


    private JDKReadDataBuffer buffer1;
    private JDKReadDataBuffer buffer2;
    private List<ByteBuffer> contentBuffers;

    public void setBuffer1(ReadDataBuffer buffer1) {
        this.buffer1 = (JDKReadDataBuffer) buffer1;
    }

    public void setBuffer2(ReadDataBuffer buffer2) {
        this.buffer2 = (JDKReadDataBuffer) buffer2;
    }

    @Override
    public int readChunkSize() throws CSException {
        assert buffer1 != null;
        assert buffer2 != null;

        return buffer1.readChunkSize() + buffer2.readChunkSize();
    }

    @Override
    public ByteBuffer chunkBuffer() {
        throw new UnsupportedOperationException("TwoReadDataBuffers is not support chunkBuffer");
    }

    @Override
    public List<ByteBuffer> contentBuffers() {
        assert contentBuffers != null;
        return contentBuffers;
    }

    @Override
    public boolean parseAndVerifyData() {

        buffer1.data.clear();
        buffer2.data.clear();
        contentBuffers = new ArrayList<>();

        while (buffer1.data.hasRemaining()) {
            var contentBuffer = buffer1.parseNext(buffer1.data, false);
            if (contentBuffer != null) {
                contentBuffers.add(contentBuffer);
            } else if (buffer1.parseResultStatus == ParseResultStatus.INCOMPLETE) {
                break;
            } else {
                log.error("verify read buffer1 data failed {}", buffer1.parseResultStatus);
                return false;
            }
        }

        if (buffer1.data.hasRemaining()) {
            var resultStatus = parseAndVerifyCrossSegmentData();
            if (resultStatus == ParseResultStatus.SUCCESS) {

            } else if (resultStatus == ParseResultStatus.INCOMPLETE) {
                log.error("verify read cross-segment data failed dataLen > max content size, current pos {} remaining {}", buffer2.data.position(), buffer2.data.remaining());
                return false;
            } else {
                log.error("verify read cross-segment data failed {}", resultStatus);
                return false;
            }
        }

        while (buffer2.data.hasRemaining()) {
            var contentBuffer = buffer2.parseNext(buffer2.data, true);
            if (contentBuffer != null) {
                contentBuffers.add(contentBuffer);
            } else {
                log.error("verify read buffer2 data failed {}", buffer2.parseResultStatus);
                return false;
            }
        }
        return true;
    }

    private ParseResultStatus parseAndVerifyCrossSegmentData() {

        int startPos1 = buffer1.data.position();
        int startPos2 = buffer2.data.position();

        // divide the data to two parts, first part belong to buffer1, second part belong to buffer2
        int crossPos = buffer1.data.remaining();

        int subContentLen;
        if (buffer1.data.remaining() >= CSConfiguration.writeSegmentHeaderLen()) {
            subContentLen = buffer1.data.getInt();
        } else {
            var sizeBuffer = ByteBuffer.allocate(4);
            sizeBuffer.put(buffer1.data);

            buffer2.data.get(sizeBuffer.array(), sizeBuffer.position(), sizeBuffer.remaining());
            sizeBuffer.position(0).limit(4);
            subContentLen = sizeBuffer.getInt();
        }

        if (subContentLen > buffer1.data.remaining() + buffer2.data.remaining() - CSConfiguration.writeSegmentFooterLen()) {
            log.error("verify read cross-segment data failed dataLen {} > max content size {}",
                      subContentLen, (buffer1.data.remaining() + buffer2.data.remaining() - CSConfiguration.writeSegmentFooterLen()));
            return ParseResultStatus.INCOMPLETE;
        }

        var crcLen = CSConfiguration.writeSegmentHeaderLen() + subContentLen;
        long retChecksum;
        if (buffer1.data.remaining() < subContentLen) {
            var size1 = buffer1.data.limit() - startPos1;
            var size2 = crcLen - size1;
            //!! this is an assumption that buffer1 and buffer2 are always the same type(Heap/Direct)
            retChecksum = buffer1.computeCRC32(buffer1.data, startPos1, size1, buffer2.data, startPos2, size2);
        } else {
            retChecksum = buffer1.computeCRC32(buffer1.data, startPos1, crcLen);
        }

        long checksum;
        if (startPos1 + crcLen + CSConfiguration.writeSegmentFooterChecksumLen() < buffer1.data.limit()) {
            buffer1.data.position(startPos1 + crcLen);
            checksum = buffer1.data.getLong();
        } else if (startPos1 + crcLen < buffer1.data.limit()) {
            buffer1.data.position(startPos1 + crcLen);
            var checksumBuffer = ByteBuffer.allocate(8);
            checksumBuffer.put(buffer1.data);
            buffer2.data.get(checksumBuffer.array(), checksumBuffer.position(), checksumBuffer.remaining());
            checksumBuffer.position(0).limit(8);
            checksum = checksumBuffer.getLong();
        } else {
            buffer1.data.position(buffer1.data.limit());
            buffer2.data.position(crcLen - crossPos);
            checksum = buffer2.data.getLong();
        }

        if (retChecksum != checksum) {
            log.error("verify read data failed return checksum {} != disk checksum {}", retChecksum, checksum);
            return ParseResultStatus.CHECKSUM_UNMATCHED;
        }

        buffer2.data.position(crcLen - crossPos + CSConfiguration.writeSegmentFooterLen());

        if (crossPos < CSConfiguration.writeSegmentHeaderLen()) {
            var buffer = buffer2.data.duplicate()
                                     .position(CSConfiguration.writeSegmentHeaderLen() - crossPos)
                                     .limit(CSConfiguration.writeSegmentHeaderLen() - crossPos + subContentLen);
            contentBuffers.add(buffer);
        } else if (crossPos >= CSConfiguration.writeSegmentHeaderLen()
                   && crossPos < crcLen) {
            var b1 = buffer1.data.duplicate()
                                 .position(startPos1 + CSConfiguration.writeSegmentHeaderLen())
                                 .limit(buffer1.data.limit());
            contentBuffers.add(b1);
            var b2 = buffer2.data.duplicate()
                                 .position(0)
                                 .limit(crcLen - crossPos);
            contentBuffers.add(b2);
        } else {
            var buffer = buffer1.data.duplicate()
                                     .position(startPos1 + CSConfiguration.writeSegmentHeaderLen())
                                     .limit(startPos1 + CSConfiguration.writeSegmentHeaderLen() + subContentLen);
            contentBuffers.add(buffer);
        }

        return ParseResultStatus.SUCCESS;
    }

    @Override
    public void put(ByteBuf in) {
        throw new UnsupportedOperationException("TwoReadDataBuffers not support put ByteBuf");
    }

    @Override
    public void put(ByteBuffer buffer) {
        throw new UnsupportedOperationException("TwoReadDataBuffers not support put ByteBuffer");
    }

    @Override
    public int writableBytes() {
        throw new UnsupportedOperationException("TwoReadDataBuffers not support writableBytes");
    }

    @Override
    public long nativeDataAddress() {
        throw new UnsupportedOperationException("TwoReadDataBuffers not support nativeDataAddress");
    }
}
