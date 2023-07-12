/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.events;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassPathResources;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.extension.AbstractExtensionLoader;
import io.cdap.cdap.spi.events.EventReader;
import io.cdap.cdap.spi.events.StartProgramEvent;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link StartProgramEventReaderExtensionProvider}
 * which provides Event reader extension classes.
 * extending from {@link AbstractExtensionLoader}
 */
public class StartProgramEventReaderExtensionProvider
        extends AbstractExtensionLoader<String, EventReader<StartProgramEvent>>
        implements EventReaderProvider<StartProgramEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(StartProgramEventReaderExtensionProvider.class);
  private static final Set<String> ALLOWED_RESOURCES = createAllowedResources();
  private static final Set<String> ALLOWED_PACKAGES = createPackageSets(ALLOWED_RESOURCES);
  private final Collection<String> enabledEventReaders;

  /**
   * Retrieve enabled event readers from cConf.
   *
   * @param cConf configuration
   */
  @Inject
  public StartProgramEventReaderExtensionProvider(CConfiguration cConf) {
    super(cConf.get(Constants.Event.START_EVENTS_READER_EXTENSIONS_DIR, ""));
    this.enabledEventReaders = cConf.getStringCollection(
            Constants.Event.START_EVENTS_READER_EXTENSIONS_ENABLED_LIST);
    if (this.enabledEventReaders == null || this.enabledEventReaders.isEmpty()) {
      return;
    }
    LOG.debug("Loaded event readers are {} .", enabledEventReaders);
  }

  private static Set<String> createAllowedResources() {
    try {
      return ClassPathResources.getResourcesWithDependencies(EventReader.class.getClassLoader(),
              EventReader.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to trace dependencies for reader extension. "
              + "Usage of events reader might fail.", e);
    }
  }

  public Map<String, EventReader<StartProgramEvent>> loadEventReaders() {
    return getAll();
  }

  @Override
  protected Set<String> getSupportedTypesForProvider(EventReader eventReader) {
    if (enabledEventReaders == null ||
            !enabledEventReaders.contains(eventReader.getClass().getName())) {
      LOG.debug("{} is not present in the allowed list of event readers.",
              eventReader.getClass().getName());
      return Collections.emptySet();
    }

    return Collections.singleton(eventReader.getClass().getName());
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
}
