package io.cdap.cdap.data.runtime;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import io.cdap.cdap.spi.metadata.MetadataStorage;

public class MetadataStorageProviderModule extends AbstractModule {

    public MetadataStorageProviderModule() {
    }

    @Override
    protected void configure() {
        bind(MetadataStorage.class).to(DefaultMetadataStorageProvider.class).in(Scopes.SINGLETON);
        bind(MetadataStorageLoader.class).in(Scopes.SINGLETON);
        bind(MetadataStorageExtensionLoader.class).in(Scopes.SINGLETON);

    }
}