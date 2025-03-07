package io.cdap.cdap.data.runtime;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.spi.metadata.MetadataStorageContext;

import java.util.Collections;
import java.util.Map;

public class DefaultMetadataStorageProviderContext implements MetadataStorageContext {

    public static final String METADATA_STORAGE_PREFIX = "metadata.storage";
    private final Map<String, String> cConf;

    private final Map<String, String> properties;

    protected DefaultMetadataStorageProviderContext(CConfiguration cConf, String storageName) {
        String prefix = String.format("%s.%s.", METADATA_STORAGE_PREFIX, storageName);
        this.cConf = Collections.unmodifiableMap(cConf.getPropsWithPrefix(prefix));
        this.properties = Collections.unmodifiableMap(cConf.getPropsWithPrefix(prefix));
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public Map<String, String> getConfiguration() {
        return cConf;
    }
}