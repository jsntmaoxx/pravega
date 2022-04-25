package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni.EC;

import java.util.concurrent.ThreadLocalRandom;

public class ECUtils {

    /*
    used for test only
     */
    public static long makeMatrix(int dataNum, int segLen, byte[] v) {
        var matrix = EC.make_buffer(dataNum, Long.BYTES);
        for (int i = 0; i < dataNum; ++i) {
            var buffer = EC.make_buffer(segLen, Byte.BYTES);
            G.U.setMemory(buffer, segLen, v[i]);
            G.U.putLong(matrix + (long) i * Long.BYTES, buffer);
        }
        return matrix;
    }

    /*
    used for test only
     */
    public static long makeMatrixWithRandomInitValue(int dataNum, int segLen) {
        var matrix = EC.make_buffer(dataNum, Long.BYTES);
        for (int i = 0; i < dataNum; ++i) {
            var buffer = EC.make_buffer(segLen, Byte.BYTES);
            var bytes = new byte[segLen];
            ThreadLocalRandom.current().nextBytes(bytes);
            for (int vi = 0; vi < segLen; ++vi) {
                G.U.putByte(buffer + vi, bytes[vi]);
            }
            G.U.putLong(matrix + (long) i * Long.BYTES, buffer);
        }
        return matrix;
    }

    public static long makeMatrix(int dataNum, int segLen, byte v) {
        var matrix = EC.make_buffer(dataNum, Long.BYTES);
        for (int i = 0; i < dataNum; ++i) {
            var buffer = EC.make_buffer(segLen, Byte.BYTES);
            G.U.setMemory(buffer, segLen, v);
            G.U.putLong(matrix + (long) i * Long.BYTES, buffer);
        }
        return matrix;
    }

    public static long makeMatrix(int dataNum, int segLen) {
        var matrix = EC.make_buffer(dataNum, Long.BYTES);
        for (int i = 0; i < dataNum; ++i) {
            var buffer = EC.make_buffer(segLen, Byte.BYTES);
            G.U.putLong(matrix + (long) i * Long.BYTES, buffer);
        }
        return matrix;
    }

    public static void setMatrix(long matrix, int dataNum, int segLen, byte v) {
        for (int i = 0; i < dataNum; ++i) {
            var buffer = G.U.getLong(matrix + (long) i * Long.BYTES);
            G.U.setMemory(buffer, segLen, v);
        }
    }

    public static void destroyMatrix(long bufferAry, int num) {
        for (int i = 0; i < num; ++i) {
            EC.destroy_buffer(G.U.getLong(bufferAry + (long) i * Long.BYTES));
        }
        EC.destroy_buffer(bufferAry);
    }

    public static String printMatrix(String name, long matrix, int x, int y) {
        y = Math.min(y, 64);
        var m = new StringBuilder().append(name).append('\n');
        for (var i = 0; i < x; ++i) {
            m.append('[');
            var row = G.U.getLong(matrix + (long) i * Long.BYTES);
            for (var j = 0; j < y; ++j) {
                m.append(Integer.toHexString(G.U.getByte(row + j) & 0xff));
                if (j + 1 < y) {
                    m.append(',');
                }
            }
            m.append("]\n");
        }
        return m.toString();
    }

    public static long makeSourceMatrix(int dataNum, int codeNum, boolean[] vSegState, long dataMatrix, long codeMatrix, int offset) throws CSException {
        var matrix = EC.make_buffer(dataNum, Long.BYTES);
        var si = 0;
        for (var i = 0; i < dataNum; ++i) {
            if (si == dataNum) {
                break;
            }
            if (vSegState[i]) {
                G.U.putLong(matrix + (long) si * Long.BYTES, G.U.getLong(dataMatrix + (long) i * Long.BYTES) + offset);
                ++si;
            }
        }
        for (var i = 0; i < codeNum; ++i) {
            if (si == dataNum) {
                break;
            }
            if (vSegState[dataNum + i]) {
                G.U.putLong(matrix + (long) si * Long.BYTES, G.U.getLong(codeMatrix + (long) i * Long.BYTES) + offset);
                ++si;
            }
        }
        if (si != dataNum) {
            throw new CSException("not enough healthy segment");
        }
        return matrix;
    }

    public static void destroySourceMatrix(long bufferAry) {
        EC.destroy_buffer(bufferAry);
    }
}
