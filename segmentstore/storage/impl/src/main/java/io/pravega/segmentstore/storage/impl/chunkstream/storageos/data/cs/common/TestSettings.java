package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

import java.nio.file.Path;
import java.nio.file.Paths;

public class TestSettings {
    public static int CreateChunkMinDelayMs = 10;
    public static int CreateChunkMaxDelayMs = 0;
    public static long WriteChunkMinDelayMs = 1;
    public static long WriteChunkMaxDelayMs = 0;
    public static boolean geoSendEnabled = false;
    public static long BlockBinSize = 10L * 1024 * 1024 * 1024;
    public static Path ChunkRootPath = Paths.get("chunkCache");
    public static String NVMeLocalServerUDSPath = "/tmp/nvme_local_server_uds.socket";
}
