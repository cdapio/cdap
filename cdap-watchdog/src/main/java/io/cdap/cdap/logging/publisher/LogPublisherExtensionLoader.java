/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.logging.publisher;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassPathResources;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.extension.AbstractExtensionLoader;
import io.cdap.cdap.spi.logs.LogPublisher;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extension loader for {@link LogPublisher} implementations.
 */
public class LogPublisherExtensionLoader extends AbstractExtensionLoader<String, LogPublisher> {

  private static final Logger LOG = LoggerFactory.getLogger(LogPublisherExtensionLoader.class);
  private static final Set<String> ALLOWED_RESOURCES = createAllowedResources();
  private static final Set<String> ALLOWED_PACKAGES = createPackageSets(ALLOWED_RESOURCES);

  private final boolean isLogPublisherEnabled;

  /**
   * Constructs a {@link LogPublisherExtensionLoader} to manage the loading of log publisher
   * extensions.
   *
   * @param cConf The configuration object containing properties for loading log publisher
   *              extensions.
   */
  @Inject
  public LogPublisherExtensionLoader(CConfiguration cConf) {
    super(cConf.get(Constants.Logging.LOG_PUBLISHER_EXT_DIR));
    this.isLogPublisherEnabled = cConf.getBoolean(Constants.Logging.LOG_PUBLISHER_ENABLED);
    LOG.debug("Log Publisher is {}.", this.isLogPublisherEnabled ? "enabled" : "not enabled");
  }

  private static Set<String> createAllowedResources() {
    try {
      return ClassPathResources.getResourcesWithDependencies(LogPublisher.class.getClassLoader(),
          LogPublisher.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to trace dependencies for Log Publisher extension.", e);
    }
  }

  @Override
  protected Set<String> getSupportedTypesForProvider(LogPublisher logPublisher) {
    if (isLogPublisherEnabled) {
      return Collections.singleton(logPublisher.getName());
    }

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
