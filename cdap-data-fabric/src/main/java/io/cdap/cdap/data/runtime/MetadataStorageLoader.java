package io.cdap.cdap.data.runtime; // Or your package

import io.cdap.cdap.spi.metadata.MetadataStorage;

import java.util.Map;

public interface MetadataStorageLoader {

    Map<String, MetadataStorage> loadProviders();
}