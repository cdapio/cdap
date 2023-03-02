/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.metrics.process.loader;

import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassPathResources;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.extension.AbstractExtensionLoader;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extension loader to load log appenders.
 */
public class MetricsWriterExtensionLoader extends AbstractExtensionLoader<String, MetricsWriter>
    implements MetricsWriterProvider {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsWriterExtensionLoader.class);
  private static final Set<String> ALLOWED_RESOURCES = createAllowedResources();
  private static final Set<String> ALLOWED_PACKAGES = createPackageSets(ALLOWED_RESOURCES);
  private Collection<String> enabledMetricsWriters;

  private static Set<String> createAllowedResources() {
    try {
      return ClassPathResources.getResourcesWithDependencies(MetricsWriter.class.getClassLoader(),
          MetricsWriter.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to trace dependencies for provisioner extension. "
          + "Usage of metrics writer might fail.", e);
    }
  }

  @Inject
  public MetricsWriterExtensionLoader(CConfiguration cConf) {
    super(cConf.get(Constants.Metrics.METRICS_WRITER_EXTENSIONS_DIR));
    this.enabledMetricsWriters = cConf.getStringCollection(
        Constants.Metrics.METRICS_WRITER_EXTENSIONS_ENABLED_LIST);
    if (this.enabledMetricsWriters == null || this.enabledMetricsWriters.isEmpty()) {
      LOG.debug("No metric writers enabled.");
      return;
    }
    LOG.debug("Enabled metric writers are {} .", enabledMetricsWriters);
  }

  @Override
  protected Set<String> getSupportedTypesForProvider(MetricsWriter metricsWriter) {
    if (enabledMetricsWriters == null || !enabledMetricsWriters.contains(metricsWriter.getID())) {
      LOG.debug("{} is not present in the enabled list of metric writers.", metricsWriter.getID());
      return Collections.emptySet();
    }

    return Collections.singleton(metricsWriter.getID());
  }

  @Override
  protected FilterClassLoader.Filter getExtensionParentClassLoaderFilter() {
    // Only allow spi classes.
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
  public Map<String, MetricsWriter> loadMetricsWriters() {
    return getAll();
  }
}
