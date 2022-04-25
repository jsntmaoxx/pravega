/**
 * Copyright (c) 2012 EMC Corporation
 * All Rights Reserved
 * <p>
 * This software contains the intellectual property of EMC Corporation
 * or is licensed to EMC Corporation from third parties.  Use of this
 * software and the intellectual property contained therein is expressly
 * limited to the terms and conditions of the License Agreement under which
 * it is provided by or on behalf of EMC.
 */

package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.atlas.common;

import io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common.CSRuntimeException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Default configuration implementation
 */
public class ConfigurationImpl implements Configuration {
    private static final char RESERVED_CHAR = '_';
    private static final String KIND_KEY = "_kind";
    private static final String ID_KEY = "_id";

    private Properties _map = new Properties();

    /**
     * Deserializes configuration object
     *
     * @param content
     * @return
     */
    public static Configuration parse(byte[] content) {
        try {
            Properties p = new Properties();
            p.load(new ByteArrayInputStream(content));
            ConfigurationImpl config = new ConfigurationImpl();
            config._map = p;
            return config;
        } catch (IOException e) {
            throw new CSRuntimeException("Fail to parse bytes as configuration", e);
        }
    }

    /**
     * Deserializes configuration object
     *
     * @param content
     * @return
     */
    public static Configuration parse(InputStream content) {
        try {
            Properties p = new Properties();
            p.load(content);
            ConfigurationImpl config = new ConfigurationImpl();
            config._map = p;
            return config;
        } catch (IOException e) {
            throw new CSRuntimeException("Fail to parse input stream as configuration", e);
        }
    }

    @Override
    public String getKind() {
        return _map.getProperty(KIND_KEY);
    }

    public void setKind(String kind) {
        _map.setProperty(KIND_KEY, kind);
    }

    @Override
    public String getId() {
        return _map.getProperty(ID_KEY);
    }

    public void setId(String id) {
        _map.setProperty(ID_KEY, id);
    }

    @Override
    public String getConfig(String key) {
        return _map.getProperty(key);
    }

    @Override
    public Map<String, String> getAllConfigs(boolean customOnly) {
        Map<String, String> toReturn = new HashMap<String, String>();
        for (Entry<Object, Object> e : _map.entrySet()) {
            String k = (String) e.getKey();
            String v = (String) e.getValue();
            if (!customOnly || (customOnly && !k.equals(KIND_KEY) && !k.equals(ID_KEY))) {
                toReturn.put(k, v);
            }
        }
        return toReturn;
    }

    @Override
    public void setConfig(String key, String val) {
        if (key == null || key.length() == 0 || key.charAt(0) == RESERVED_CHAR) {
            throw new CSRuntimeException("Invalid config key");
        }
        _map.setProperty(key, val);
    }

    @Override
    public Object removeConfig(String key) {
        if (key == null || key.length() == 0 || key.charAt(0) == RESERVED_CHAR) {
            throw new CSRuntimeException("Invalid config key");
        }
        return _map.remove(key);
    }

    @Override
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        _map.store(out, null);
        return out.toByteArray();
    }
}
