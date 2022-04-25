package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.policy;

import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.Arrays;
import java.util.stream.Collectors;

public class HEXEncodePolicy extends AbstractEncodePolicy {

    @Override
    protected String[] parseKey(String key) {
        var keys = key.split("/");
        var part = Arrays.stream(keys).filter(k -> k.startsWith("0x")).map(k -> k.substring(2)).collect(Collectors.joining());
        if (part.length() != 27) {
            throw new IllegalArgumentException("key parts is not 108 bits, please check");
        }
        String[] parts = new String[3];
        parts[0] = part.substring(0, CONTAINERID_LENGTH);
        parts[1] = part.substring(CONTAINERID_LENGTH, CONTAINERID_LENGTH + SIMID_LENGTH);
        parts[2] = part.substring(CONTAINERID_LENGTH + SIMID_LENGTH);
        return parts;
    }

    @Override
    protected ImmutablePair<String[], Integer> parsePrefix(String prefix) {
        var keys = prefix.split("/");
        var part = Arrays.stream(keys).filter(k -> k.startsWith("0x")).map(k -> k.substring(2)).collect(Collectors.joining());
        if (part.length() > CONTAINERID_LENGTH + SIMID_LENGTH + BLOBID_LENGTH) {
            throw new IllegalArgumentException("prefix exceeds 108 bits, please check");
        }
        if (part.length() <= CONTAINERID_LENGTH) {
            var paddingPart = appendingPart(part, CONTAINERID_LENGTH - part.length());
            return new ImmutablePair<>(new String[]{paddingPart}, part.length() > 3 ? part.length() + BUCKET_INDEX_LENGTH + 1 : part.length() + BUCKET_INDEX_LENGTH);
        } else if (part.length() > CONTAINERID_LENGTH && part.length() <= CONTAINERID_LENGTH + SIMID_LENGTH) {
            var paddingPart = appendingPart(part, CONTAINERID_LENGTH + SIMID_LENGTH - part.length());
            return new ImmutablePair<>(new String[]{paddingPart.substring(0, CONTAINERID_LENGTH), paddingPart.substring(CONTAINERID_LENGTH)}, part.length() + BUCKET_INDEX_LENGTH + 2);
        } else {
            var paddingPart = appendingPart(part, CONTAINERID_LENGTH + SIMID_LENGTH + BLOBID_LENGTH - part.length());
            return new ImmutablePair<>(new String[]{paddingPart.substring(0, CONTAINERID_LENGTH), paddingPart.substring(CONTAINERID_LENGTH, CONTAINERID_LENGTH + SIMID_LENGTH), paddingPart.substring(CONTAINERID_LENGTH + SIMID_LENGTH)}, part.length() > 15 ? part.length() + BUCKET_INDEX_LENGTH + 4 : part.length() + BUCKET_INDEX_LENGTH + 3);
        }
    }

    private String appendingPart(String part, int number) {
        StringBuilder paddingPart = new StringBuilder();
        paddingPart.append(part);
        for (int i = 0; i < number; i++) {
            paddingPart.append('0');
        }
        return paddingPart.toString();
    }

}
