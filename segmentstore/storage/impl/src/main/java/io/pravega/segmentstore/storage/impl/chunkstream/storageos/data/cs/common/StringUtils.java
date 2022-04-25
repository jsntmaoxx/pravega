package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.HeapWriteDataBuffer;
import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.writer.buffer.WriteDataBuffer;
import org.apache.commons.codec.binary.Hex;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

public class StringUtils {
    public static final String HeaderRange = "Range";
    public static final String HeaderEMCExtensionLocation = "x-emc-location";
    public static final String HeaderEMCExtensionLocationType = "x-emc-location-type";
    public static final String HeaderTestIndexGranularity = "x-test-index-granularity";

    public static final WriteDataBuffer dumbBuffer = HeapWriteDataBuffer.allocate(0, 1024 * 1024, 0, 0);
    private static final String[] SizeUnit =
            {
                    "B",
                    "K",
                    "M",
                    "G",
                    "T",
                    "P",
                    "E",
                    };

    private static final String[] DurationUnit =
            {
                    "S",
                    "M",
                    "H",
                    };

    public static String size2String(long size) {
        int ui = 0;
        long uSize = size;
        while (uSize % 1024 == 0 && (uSize = uSize / 1024) > 0 && ui < SizeUnit.length - 1) {
            ++ui;
        }
        return String.format("%d%s", uSize, SizeUnit[ui]);
    }

    public static String size2String(double size) {
        int ui = 0;
        double dSize = size;
        double uSize = size;
        while ((uSize = uSize / 1024.0) > 1.0 + 1e-6 && ui < SizeUnit.length - 1) {
            ++ui;
            dSize = dSize / 1024.0;
        }
        return String.format("%.2f%s", dSize, SizeUnit[ui]);
    }

    public static long strSize2Long(String strCapacity) {
        int length = strCapacity.length();
        long cap = length > 0 ? Long.parseLong(strCapacity.substring(0, length - 1)) : 0;
        switch (strCapacity.charAt(length - 1)) {
            case 'b':
            case 'B':
                break;
            case 'k':
            case 'K':
                cap = cap * 1024L;
                break;
            case 'm':
            case 'M':
                cap = cap * 1024 * 1024L;
                break;
            case 'g':
            case 'G':
                cap = cap * 1024 * 1024 * 1024L;
                break;
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                cap = Long.parseLong(strCapacity);
                break;
        }
        return cap;
    }

    public static int strSize2Int(String strCapacity) {
        var length = strCapacity.length();
        var cap = length > 0 ? Integer.parseInt(strCapacity.substring(0, length - 1)) : 0;
        switch (strCapacity.charAt(length - 1)) {
            case 'b':
            case 'B':
                break;
            case 'k':
            case 'K':
                cap = cap * 1024;
                break;
            case 'm':
            case 'M':
                cap = cap * 1024 * 1024;
                break;
            case 'g':
            case 'G':
                cap = cap * 1024 * 1024 * 1024;
                break;
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                cap = Integer.parseInt(strCapacity);
                break;
        }
        return cap;
    }

    public static double strSize2Double(String strCapacity) {
        int length = strCapacity.length();
        var cap = length > 0 ? Double.parseDouble(strCapacity.substring(0, length - 1)) : 0;
        switch (strCapacity.charAt(length - 1)) {
            case 'b':
            case 'B':
                break;
            case 'k':
            case 'K':
                cap = cap * 1024L;
                break;
            case 'm':
            case 'M':
                cap = cap * 1024 * 1024L;
                break;
            case 'g':
            case 'G':
                cap = cap * 1024 * 1024 * 1024L;
                break;
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                cap = Double.parseDouble(strCapacity);
                break;
        }
        return cap;
    }

    public static int duration2Seconds(String durationStr) {
        var length = durationStr.length();
        var seconds = Double.parseDouble(durationStr.substring(0, length - 1));
        switch (durationStr.charAt(length - 1)) {
            case 's':
            case 'S':
                break;
            case 'm':
            case 'M':
                seconds = seconds * 60;
                break;
            case 'h':
            case 'H':
                seconds = seconds * 60 * 60;
                break;
        }
        return (int) seconds;
    }

    public static String duration2String(int durationSeconds) {
        int ui = 0;
        int uSecond = durationSeconds;
        while (uSecond % 60 == 0 && (uSecond = uSecond / 60) > 0 && ui < DurationUnit.length - 1) {
            ++ui;
        }
        return String.format("%d%s", uSecond, DurationUnit[ui]);
    }

    public static String md5Base64(byte[] md5Bytes) {
        return Base64.getEncoder().encodeToString(md5Bytes);
    }

    public static String md5Hex(byte[] md5Bytes) {
        return Hex.encodeHexString(md5Bytes);
    }

    public static byte[] md5Bytes(byte[] content) {
        try {
            MessageDigest md = MessageDigest.getInstance("md5");
            md.update(content);
            return md.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new CSRuntimeException("no md5 algorithm");
        }
    }

    public static List<Integer> parseCTIndexes(String ctIndexesStr) throws CSException {
        if (ctIndexesStr == null || ctIndexesStr.isEmpty()) {
            return Collections.emptyList();
        }

        List<Integer> indexes = new ArrayList<>();
        String[] subs = ctIndexesStr.split(",");
        for (String sub : subs) {
            if (!sub.contains("-")) {
                indexes.add(Integer.parseInt(sub));
                continue;
            }
            String[] intervals = sub.split("-");
            if (intervals.length != 2) {
                throw new CSException("invalid CT indexes interval, expected style is 1,2-5,10");
            }
            int start = Integer.parseInt(intervals[0]);
            int end = Integer.parseInt(intervals[1]);
            if (start > end) {
                throw new CSException("invalid CT indexes interval, expected style is 1,2-5,10");
            }
            for (int i = start; i <= end; ++i) {
                indexes.add(i);
            }
        }

        return indexes;
    }

    public static String bytes2String(byte[] buf) {
        var p = 0;
        while (p < buf.length && buf[p] != 0) {
            ++p;
        }
        return new String(buf, 0, p, StandardCharsets.UTF_8);
    }

    static int roundToFloorPowerOf2(double iv) {
        var v = 1;
        while (iv > v * 2) {
            v *= 2;
        }
        return v;
    }
}
