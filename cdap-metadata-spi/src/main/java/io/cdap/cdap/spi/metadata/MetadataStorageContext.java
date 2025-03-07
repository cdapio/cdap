package io.cdap.cdap.spi.metadata;

import java.util.Map;

public interface MetadataStorageContext {
    Map<String, String> getProperties();

    /**
     * Configurations for the storage provider. It contains all the CDAP configurations that are
     * prefixed with {@code data.storage.properties.[storage_provider_name].} with the prefixed
     * stripped.
     */
    Map<String, String> getConfiguration();

}