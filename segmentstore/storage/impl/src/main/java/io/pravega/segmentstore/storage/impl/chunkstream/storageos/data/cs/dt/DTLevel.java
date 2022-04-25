package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSRuntimeException;

public enum DTLevel {
    Level0(0),
    Level1(1),
    Level2(2);

    public static final int LevelCount = 3;
    private final int level;

    DTLevel(int level) {
        this.level = level;
    }

    public static DTLevel valueOf(int v) {
        switch (v) {
            case 0:
                return Level0;
            case 1:
                return Level1;
            case 2:
                return Level2;
            default:
                throw new CSRuntimeException("unknown level " + v);
        }
    }

    public int intLevel() {
        return this.level;
    }
}
