/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.internal.provision;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.runtime.spi.provisioner.Provisioner;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the ProvsionerConfigProvider. It expects a json file for from each
 * module dir and expects a "configuration-groups" in the json file. "icon" and "beta" fields are
 * optional.
 */
public class DefaultProvisionerConfigProvider implements ProvisionerConfigProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultProvisionerConfigProvider.class);
  private static final Gson GSON = new GsonBuilder().create();

  @Override
  public Map<String, ProvisionerConfig> loadProvisionerConfigs(
      Collection<Provisioner> provisioners) {
    Map<String, ProvisionerConfig> results = new HashMap<>();

    for (Provisioner provisioner : provisioners) {
      String provisionerName = provisioner.getSpec().getName();

      try (Reader reader = openConfigReader(provisioner)) {
        if (reader == null) {
          continue;
        }
        results.put(provisionerName,
            GSON.fromJson(new JsonReader(reader), ProvisionerConfig.class));
      } catch (Exception e) {
        LOG.warn("Exception reading JSON config file for provisioner {}. Ignoring file.",
            provisionerName, e);
      }
    }

    return Collections.unmodifiableMap(results);
  }

  /**
   * Find the json config file with name [provisioner_name].json under the provisioner ext
   * directory. The ext directory can be found by the classloader of the provisioner. If the json
   * config file is not found in the ext directory, try to locate it from the provisioner
   * classloader.
   *
   * @param provisioner the {@link Provisioner}
   * @return a {@link Reader} for reading the config file, or {@code null} if the config file cannot
   *     be located
   */
  @Nullable
  private Reader openConfigReader(Provisioner provisioner) throws IOException, URISyntaxException {
    Class<? extends Provisioner> provisionerClass = provisioner.getClass();
    URL classPathURL = ClassLoaders.getClassPathURL(provisionerClass);

    // This shouldn't happen as we should be able to find the class from its own classloader
    if (classPathURL == null) {
      LOG.warn("Unable to find provisioner class {} from its own classloader", provisionerClass);
      return null;
    }

    // Find the json config file with name [provisioner_name].json under the provisioner ext directory.
    // The ext directory can be found by the classloader of the provisioner.
    // See AbstractExtensionLoader class for the directory structure
    String provisionerName = provisioner.getSpec().getName();
    File configFileDir = new File(classPathURL.toURI());

    // If the class is loaded from a (jar) file,
    // then the .json config file is expected to be at the parent directory of it.
    if (Files.isRegularFile(configFileDir.toPath())) {
      configFileDir = configFileDir.getParentFile();
    }

    Path configFilePath = new File(configFileDir, provisionerName + ".json").toPath();
    if (Files.isRegularFile(configFilePath)) {
      return Files.newBufferedReader(configFilePath, StandardCharsets.UTF_8);
    }

    // If we cannot find the config file in the filesystem, try to look for the json config from the classloader itself.
    // This is the case when the .json is packaged inside the jar (e.g. system provisioners).
    URL resource = provisionerClass.getClassLoader().getResource(provisionerName + ".json");
    if (resource != null) {
      return new InputStreamReader(resource.openStream(), StandardCharsets.UTF_8);
    }

    LOG.debug("Unable to find JSON config file {} for provisioner {}", configFilePath,
        provisionerName);
    return null;
  }
}
