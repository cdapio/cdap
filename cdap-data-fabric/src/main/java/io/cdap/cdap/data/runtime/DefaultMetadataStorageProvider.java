package io.cdap.cdap.data.runtime;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataChange;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.MutationOptions;
import io.cdap.cdap.spi.metadata.Read;
import io.cdap.cdap.spi.metadata.SearchRequest;

import java.io.IOException;
import java.util.List;

public class DefaultMetadataStorageProvider implements MetadataStorage {

    private final CConfiguration cConf;
    private final  MetadataStorageExtensionLoader extensionLoader;

    private volatile MetadataStorage delegate;

    @Inject
    DefaultMetadataStorageProvider(CConfiguration cConf, MetadataStorageExtensionLoader extensionLoader)
            throws Exception {
        this.cConf=cConf;
        this.extensionLoader = extensionLoader;
        this.extensionLoader.getAll();

        String providerName = "spanner";

        this.delegate = this.extensionLoader.get(providerName);
        if (this.delegate == null) {
            throw new IllegalArgumentException("Unsupported MetadataProvider type: " + providerName);
        }
        this.delegate.initialize(new DefaultMetadataStorageProviderContext(cConf, providerName));
    }


    @Override
    public void createIndex() throws IOException {
        delegate.createIndex();
    }

    @Override
    public void close() {
        delegate.close();
    }

    public Object getDatasetMetadata(String datasetName) {
        return delegate.getDatasetMetadata(datasetName);
    }

    public String getName() {
        return delegate.getName();
    }

    @Override
    public void dropIndex() throws IOException {
        delegate.dropIndex();
    }

    @Override
    public MetadataChange apply(MetadataMutation mutation, MutationOptions options)
            throws IOException {
        return delegate.apply(mutation, options);
    }

    @Override
    public List<MetadataChange> batch(List<? extends MetadataMutation> mutations,
                                      MutationOptions options) throws IOException {
        return delegate.batch(mutations, options);
    }

    @Override
    public Metadata read(Read read) throws IOException {
        return delegate.read(read);
    }

    @Override
    public io.cdap.cdap.spi.metadata.SearchResponse search(SearchRequest request)
            throws IOException {
        return delegate.search(request);
    }
}
