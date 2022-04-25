package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.dt;

public enum DTType {
    OB("OB"),
    LS("LS"),
    RR("RR"),
    BR("BR"),
    CT("CT"),
    SS("SS"),
    PR("PR"),
    RT("RT"),
    MT("MT"),
    MA("MA"),
    MR("MR"),
    ET("ET");
    private final String name;

    DTType(String name) {
        this.name = name;
    }

    public String toString() {
        return this.name;
    }
}
