/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.internal.metadata;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassPathResources;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.extension.AbstractExtensionLoader;
import io.cdap.cdap.spi.metadata.MetadataConsumer;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads all MetadataConsumers from the extensions directory, if available.
 */
public class MetadataConsumerExtensionLoader extends
    AbstractExtensionLoader<String, MetadataConsumer>
    implements MetadataConsumerProvider {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataConsumerExtensionLoader.class);
  private static final Set<String> ALLOWED_RESOURCES = createAllowedResources();
  private static final Set<String> ALLOWED_PACKAGES = createPackageSets(ALLOWED_RESOURCES);
  private Collection<String> enabledMetadataConsumers;

  @Inject
  public MetadataConsumerExtensionLoader(CConfiguration cConf) {
    super(cConf.get(Constants.MetadataConsumer.METADATA_CONSUMER_EXTENSIONS_DIR) != null
        ? cConf.get(Constants.MetadataConsumer.METADATA_CONSUMER_EXTENSIONS_DIR) : "");
    this.enabledMetadataConsumers = cConf.getStringCollection(
        Constants.MetadataConsumer.METADATA_CONSUMER_EXTENSIONS_ENABLED_LIST);
    if (this.enabledMetadataConsumers == null || this.enabledMetadataConsumers.isEmpty()) {
      LOG.debug("No metadata consumers enabled.");
      return;
    }
    LOG.debug("Enabled metadata consumers are {} .", enabledMetadataConsumers);
  }

  private static Set<String> createAllowedResources() {
    try {
      return ClassPathResources.getResourcesWithDependencies(
          MetadataConsumer.class.getClassLoader(),
          MetadataConsumer.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to trace dependencies for metadata consumer extension. "
          + "Usage of metadata consumer might fail.", e);
    }
  }

  @Override
  protected Set<String> getSupportedTypesForProvider(MetadataConsumer metadataConsumer) {
    if (enabledMetadataConsumers == null || !enabledMetadataConsumers.contains(
        metadataConsumer.getName())) {
      LOG.debug("{} is not present in the enabled list of metadata consumers.",
          metadataConsumer.getName());
      return Collections.emptySet();
    }
    return Collections.singleton(metadataConsumer.getName());
  }

  @Override
  protected FilterClassLoader.Filter getExtensionParentClassLoaderFilter() {
    // filter classes to provide isolation from CDAP's classes.
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

  @Override
  public Map<String, MetadataConsumer> loadMetadataConsumers() {
    return getAll();
  }
}
