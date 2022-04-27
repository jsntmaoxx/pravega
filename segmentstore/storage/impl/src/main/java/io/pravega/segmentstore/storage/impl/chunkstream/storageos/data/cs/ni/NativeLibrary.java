package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.ni;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

public class NativeLibrary {
    private static final Logger log = LoggerFactory.getLogger("JEC");
    private static final String[] libs = {
            "libnuma.so",
            "libcs-ni.so"
    };
    private static final AtomicBoolean load = new AtomicBoolean(false);

    public static void load() {
        if (!load.compareAndSet(false, true)) {
            return;
        }
        for (String lib : libs) {
            try {
                loadLibrary(lib);
            } catch (Exception e) {
                log.error("load lib {} failed", lib);
            }
        }
    }

    private static void loadLibrary(String libName) throws IOException {
        String libJarPath = "/native/linux.x64/" + libName;
        InputStream libStream = NativeLibrary.class.getResourceAsStream(libJarPath);
        if (libStream != null) {
            File tmpDir = Files.createTempDirectory(Path.of("/tmp"), "cs-ni").toFile();
            tmpDir.deleteOnExit();
            File libFile = new File(tmpDir, libName);
            libFile.deleteOnExit();
            try {
                FileUtils.copyInputStreamToFile(libStream, libFile);
                System.load(libFile.getAbsolutePath());
                log.info("load native library {} to {}", libJarPath, libFile.getAbsoluteFile());
            } catch (Exception e) {
                log.error("load native library {} to {} failed", libJarPath, libFile.getAbsoluteFile(), e);
                e.printStackTrace();
            }
        } else {
            // ./gradlew compileJni
            // run from idea
            libJarPath = Paths.get("").toAbsolutePath() + "/cs-ni/build/native/linux.x64/" + libName;
            System.load(libJarPath);
            log.info("load native library {}", libJarPath);
        }
    }
}
