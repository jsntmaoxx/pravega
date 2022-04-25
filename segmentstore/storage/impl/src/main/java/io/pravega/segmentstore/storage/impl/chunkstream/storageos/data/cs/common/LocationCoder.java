package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

// cj_todo avoid buffer copy
// snappy and base64 encode/decode copy buffer many times
public class LocationCoder {
    private static final int BinaryMagicHead = 0xBAB01ABA;
    private static final int SnappyBinaryMagicHead = 0xBAB02ABA;
    private static final String PlainPrefix = "plain";
    private static final String JsonPrefix = "json";
    private static final String Base64BinaryPrefix = "64b";
    private static final byte[] Base64BinaryPrefixBytes = "64b".getBytes(StandardCharsets.UTF_8);
    private static final int ARRAY_BASE_OFFSET = G.U.arrayBaseOffset(byte[].class);
    private static final int ARRAY_INDEX_SCALE = G.U.arrayIndexScale(byte[].class);

    public static String encode(List<Location> writeLocations, CodeType codeType) throws CSRuntimeException {
        try {
            switch (codeType) {
                case Plain:
                    return formatSegLocations(writeLocations);
                case CompactJson:
                    return jsonCompressFormatSegLocations(writeLocations);
                default:
                case PrintJson:
                    return jsonFormatSegLocations(writeLocations);
                case Binary: {
                    var buffer = encodeSegLocations(writeLocations, BinaryMagicHead);
                    return new String(buffer.array(), buffer.arrayOffset(), buffer.limit() - buffer.position(), StandardCharsets.UTF_8);
                }
                case SnappyBinary: {
                    var buffer = encodeSnappyBinaryBuffer(writeLocations);
                    return new String(buffer.array(), buffer.arrayOffset(), buffer.limit() - buffer.position(), StandardCharsets.UTF_8);
                }
                case Base64Binary: {
                    var buffer = encodeSegLocations(writeLocations, BinaryMagicHead);
                    buffer = Base64.getEncoder().encode(buffer);
                    return Base64BinaryPrefix + new String(buffer.array(), buffer.arrayOffset(), buffer.limit() - buffer.position(), StandardCharsets.UTF_8);
                }
                case Base64SnappyBinary: {
                    var base64Buffer = Base64.getEncoder().encode(encodeSnappyBinaryBuffer(writeLocations));
                    return Base64BinaryPrefix + new String(base64Buffer.array(), base64Buffer.arrayOffset(), base64Buffer.limit() - base64Buffer.position(), StandardCharsets.UTF_8);
                }
            }
        } catch (Exception e) {
            throw new CSRuntimeException("encode location failed", e);
        }
    }

    public static void encode(OutputStream out, List<Location> writeLocations, CodeType codeType) throws IOException {
        switch (codeType) {
            case Plain:
                out.write(formatSegLocations(writeLocations).getBytes(StandardCharsets.UTF_8));
                break;
            case CompactJson:
                out.write(jsonCompressFormatSegLocations(writeLocations).getBytes(StandardCharsets.UTF_8));
                break;
            default:
            case PrintJson:
                out.write(jsonFormatSegLocations(writeLocations).getBytes(StandardCharsets.UTF_8));
                break;
            case Binary: {
                out.write(BinaryMagicHead);
                var buffer = encodeSegLocations(writeLocations, BinaryMagicHead);
                out.write(buffer.array(), buffer.arrayOffset(), buffer.limit() - buffer.position());
                break;
            }
            case SnappyBinary: {
                var buffer = encodeSnappyBinaryBuffer(writeLocations);
                out.write(buffer.array(), buffer.arrayOffset(), buffer.limit() - buffer.position());
                break;
            }
            case Base64Binary: {
                out.write(Base64BinaryPrefixBytes);
                var buffer = encodeSegLocations(writeLocations, BinaryMagicHead);
                buffer = Base64.getEncoder().encode(buffer);
                out.write(buffer.array(), buffer.arrayOffset(), buffer.limit() - buffer.position());
                break;
            }
            case Base64SnappyBinary: {
                out.write(Base64BinaryPrefixBytes);
                var base64Buffer = Base64.getEncoder().encode(encodeSnappyBinaryBuffer(writeLocations));
                out.write(base64Buffer.array(), base64Buffer.arrayOffset(), base64Buffer.limit() - base64Buffer.position());
                break;
            }
        }
    }

    public static List<Location> decode(String location) throws IOException, CSException {
        if (location.charAt(0) == '6' && location.startsWith(Base64BinaryPrefix)) {
            return decodeBinaryLocation(Base64.getDecoder().decode(ByteBuffer.wrap(location.getBytes(StandardCharsets.UTF_8)).position(Base64BinaryPrefixBytes.length)));
        }
        return decodeBinaryLocation(ByteBuffer.wrap(location.getBytes()));
    }

    public static List<Location> decode(ByteBuffer buffer) throws CSException, IOException {
        if (buffer.get() == '6' && new String(buffer.array(), buffer.arrayOffset(), Base64BinaryPrefixBytes.length).equals(Base64BinaryPrefix)) {
            buffer.position(Base64BinaryPrefixBytes.length);
            var decodeBuf = Base64.getDecoder().decode(buffer);
            return decodeBinaryLocation(decodeBuf);
        }
        return decodeBinaryLocation(buffer);
    }

    private static String formatSegLocations(List<Location> writeLocations) {
        StringBuilder str = new StringBuilder().append(PlainPrefix);
        for (var location : writeLocations) {
            str.append("\t")
               .append(location.chunkId.toString()).append(" [")
               .append(location.offset).append(",")
               .append(location.length + location.offset).append(") ")
               .append(location.length)
               .append(" logical [")
               .append(location.logicalOffset()).append(",")
               .append(location.logicalLength() + location.logicalOffset()).append(") ")
               .append(location.logicalLength());
        }
        return str.toString();
    }

    private static String jsonFormatSegLocations(List<Location> writeLocations) {
        StringBuilder str = new StringBuilder().append(JsonPrefix).append("{\n\t\"segments\": [");
        boolean first = true;
        for (var location : writeLocations) {
            if (!first) {
                str.append(",");
            } else {
                first = false;
            }
            str.append("\n\t\t{")
               .append("\n\t\t\t\"chunk\": \"").append(location.chunkId.toString()).append("\",")
               .append("\n\t\t\t\"offset\": ").append(location.offset).append(",")
               .append("\n\t\t\t\"length\": ").append(location.length)
               .append("\n\t\t\t\"logical_offset\": ").append(location.logicalOffset()).append(",")
               .append("\n\t\t\t\"logical_length\": ").append(location.logicalLength())
               .append("\n\t\t}");
        }
        str.append("\n\t]\n}");
        return str.toString();
    }

    private static String jsonCompressFormatSegLocations(List<Location> writeLocations) {
        StringBuilder str = new StringBuilder().append("json{\"segments\":[");
        boolean first = true;
        for (var location : writeLocations) {
            if (!first) {
                str.append(",");
            } else {
                first = false;
            }
            str.append("{")
               .append("\"chunk\":\"").append(location.chunkId.toString()).append("\",")
               .append("\"offset\":").append(location.offset).append(",")
               .append("\"length\":").append(location.length)
               .append("\"logical_offset\":").append(location.logicalOffset()).append(",")
               .append("\"logical_length\":").append(location.logicalLength())
               .append("}");
        }
        str.append("]}");
        return str.toString();
    }

    private static ByteBuffer encodeSegLocations(List<Location> writeLocations, int magicHeader) {
        var out = ByteBuffer.allocate(Integer.BYTES + writeLocations.size() * Location.encodeSize());
        out.putInt(magicHeader);
        out.position(Integer.BYTES);
        for (var seg : writeLocations) {
            seg.writeTo(out);
        }
        return out.flip();
    }

    private static ByteBuffer encodeSegLocations(List<Location> writeLocations) {
        var out = ByteBuffer.allocate(writeLocations.size() * Location.encodeSize());
        for (var seg : writeLocations) {
            seg.writeTo(out);
        }
        return out.flip();
    }

    private static List<Location> decodeSegLocations(ByteBuffer location) {
        var count = (location.limit() - location.position()) / Location.encodeSize();
        if (count == 1) {
            return Collections.singletonList(Location.parseFromByteBuffer(location));
        }
        var segs = new ArrayList<Location>(count);
        for (var i = 0; i < count; ++i) {
            segs.add(Location.parseFromByteBuffer(location));
        }
        return segs;
    }

    private static ByteBuffer encodeSnappyBinaryBuffer(List<Location> writeLocations) throws IOException {
        var buffer = encodeSegLocations(writeLocations);
        byte[] snappyBuf = new byte[Snappy.maxCompressedLength(buffer.limit() - buffer.position()) + Integer.BYTES];
        var snappyLen = Snappy.compress(buffer.array(), buffer.arrayOffset() + Integer.BYTES, buffer.limit() - buffer.position(), snappyBuf, 0);
        G.U.putInt(snappyBuf, ARRAY_BASE_OFFSET, SnappyBinaryMagicHead);
        return ByteBuffer.wrap(snappyBuf, 0, snappyLen);
    }

    private static List<Location> decodeBinaryLocation(ByteBuffer decodeBuf) throws IOException, CSException {
        var magicHeader = decodeBuf.getInt();
        if (magicHeader == SnappyBinaryMagicHead) {
            byte[] snappyBuf = new byte[Snappy.uncompressedLength(decodeBuf.array(), decodeBuf.arrayOffset() + decodeBuf.position(), decodeBuf.limit() - decodeBuf.position())];
            var snappyLen = Snappy.uncompress(decodeBuf.array(), decodeBuf.arrayOffset() + decodeBuf.position(), decodeBuf.limit() - decodeBuf.position(), snappyBuf, 0);
            return decodeSegLocations(ByteBuffer.wrap(snappyBuf, 0, snappyLen));
        } else if (magicHeader == BinaryMagicHead) {
            return decodeSegLocations(ByteBuffer.wrap(decodeBuf.array(), decodeBuf.arrayOffset() + decodeBuf.position(), decodeBuf.limit() - decodeBuf.position()));
        } else {
            throw new CSException("Decode location string failed");
        }
    }

    public enum CodeType {
        Plain,
        CompactJson,
        PrintJson,
        Binary,
        SnappyBinary,
        Base64Binary,
        Base64SnappyBinary,
    }
}
