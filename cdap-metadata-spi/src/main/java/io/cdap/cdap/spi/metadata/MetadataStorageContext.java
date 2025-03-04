package io.cdap.cdap.spi.metadata;

import java.util.Map;

public interface MetadataStorageContext {
    Map<String, String> getProperties();

}