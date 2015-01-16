/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Utility service that provides access to adapters currently registered
 */
public class AdapterService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(AdapterService.class);
  private final CConfiguration configuration;
  private Map<String, File> adapters;

  @Inject
  public AdapterService(CConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting AdapterService");
    adapters = registerAdapters();
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down AdapterService");
  }

  public Map<String, File> getAdapters() {
    return adapters;
  }

  public boolean isValidAdapater(String adapterType) {
    if (adapters.containsKey(adapterType)) {
      return true;
    }

    return false;
  }

  public File getAdapterLocation(String adapterType) {
    if (adapters.containsKey(adapterType)) {
      return adapters.get(adapterType);
    }
    throw new RuntimeException(String.format("Jar for adapterType %s not found", adapterType));
  }

  private Map<String, File> registerAdapters() {
    ImmutableMap.Builder<String, File> builder = ImmutableMap.builder();
    Collection<File> files = Collections.EMPTY_LIST;
    try {
      File baseDir = new File(configuration.get(Constants.AppFabric.ADAPTER_DIR));
      files = FileUtils.listFiles(baseDir, new String[]{"jar"}, true);

    } catch (Exception e) {
      LOG.warn("Unable to read the plugins directory ");
    }

    for (File file : files) {
      try {
        Manifest manifest = new JarFile(file.getAbsolutePath()).getManifest();
        if (manifest != null) {
          Attributes mainAttributes = manifest.getMainAttributes();
          String adapterType = mainAttributes.getValue("CDAP-Adapter-Type");
          if (adapterType != null) {
            builder.put(adapterType, file);
          }
        }
      } catch (IOException e) {
        LOG.warn(String.format("Unable to read plugin jar %s", file.getAbsolutePath()));
      }

    }
    return builder.build();
  }

}
