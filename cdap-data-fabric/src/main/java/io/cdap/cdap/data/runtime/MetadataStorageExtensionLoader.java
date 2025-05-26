package io.cdap.cdap.data.runtime;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassPathResources;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.extension.AbstractExtensionLoader;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 * Extension loader for {@link MetadataStorage} implementations.
 */
public class MetadataStorageExtensionLoader extends AbstractExtensionLoader<String, MetadataStorage>
         {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataStorageExtensionLoader.class);
    private static final Set<String> ALLOWED_RESOURCES = createAllowedResources();
    private static final Set<String> ALLOWED_PACKAGES = createPackageSets(ALLOWED_RESOURCES);

    private final boolean ismetadatastorageEnabled;

    /**
     * Constructs a {@link MetadataStorageExtensionLoader} to manage the loading of SpannerMetadata
     * extensions.
     *
     * @param cConf The configuration object containing properties for loading SpannerMetadata
     *              extensions.
     */
    @Inject
    public MetadataStorageExtensionLoader(CConfiguration cConf) {
        super(cConf.get(Constants.Metadata.Metadata_Storage_EXT_DIR));
        this.ismetadatastorageEnabled = cConf.getBoolean(Constants.Metadata.Metadata_Storage_Enabled);
        LOG.debug("Metadata Storage extensions directory: {}",
                cConf.get(Constants.Metadata.Metadata_Storage_EXT_DIR));
    }

    private static Set<String> createAllowedResources() {
        try {
            return ClassPathResources.getResourcesWithDependencies(MetadataStorage.class.getClassLoader(),
                    MetadataStorage.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to trace dependencies for MetadataStorage extension.", e);
        }
    }

    @Override
    protected Set<String> getSupportedTypesForProvider(MetadataStorage metadataStorage) {
        if (ismetadatastorageEnabled) {
            LOG.info("metadataStorage is not Empty",Collections.singleton(metadataStorage.getName()));
            return Collections.singleton(metadataStorage.getName());
        }
        LOG.info("metadataStorage is Empty");
        return Collections.emptySet();
    }

    @Override
    protected FilterClassLoader.Filter getExtensionParentClassLoaderFilter() {
        return new FilterClassLoader.Filter() {
            @Override
            public boolean acceptResource(String resource) {
                return ALLOWED_RESOURCES.contains(resource);
            }

            @Override
            public boolean acceptPackage(String packageName) {
                return ALLOWED_PACKAGES.contains(packageName);
            }
        };
    }
}
