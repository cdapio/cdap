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

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;

/**
 * Default implementation of the ProvisionerConfigProvider. It expects a json file for from each module dir and
 * expects a "configuration-groups" in the json file. "icon" and "beta" fields are optional.
 */
public class DefaultProvisionerConfigProvider implements ProvisionerConfigProvider {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultProvisionerConfigProvider.class);
  private static final Gson GSON = new GsonBuilder().create();

  private List<String> extDirs;

  @Inject
  public DefaultProvisionerConfigProvider(CConfiguration cConf) {
    String extDirectory = cConf.get(Constants.Provisioner.EXTENSIONS_DIR);
    this.extDirs = ImmutableList.copyOf(Splitter.on(';').omitEmptyStrings().trimResults().split(extDirectory));
  }

  /**
   * First tries to load configs by searching through each module in the extensions directory. Then uses system
   * classloader to find the remaining json files.
   */
  @Override
  public Map<String, ProvisionerConfig> loadProvisionerConfigs(Set<String> provisioners) {
    Map<String, ProvisionerConfig> results = new HashMap<>();

    for (String dir : extDirs) {
      File extDir = new File(dir);
      if (!extDir.isDirectory()) {
        continue;
      }

      List<File> files = new ArrayList<>(DirUtils.listFiles(extDir));
      Collections.sort(files);
      for (File moduleDir : files) {
        if (!moduleDir.isDirectory()) {
          continue;
        }

        List<File> jsonFiles =
          DirUtils.listFiles(moduleDir,
                             file -> {
                               String name = file.getName();
                               return !file.isDirectory() && name.endsWith(".json")
                                 && provisioners.contains(name.substring(0, name.lastIndexOf('.')));
                             });
        if (jsonFiles.isEmpty()) {
          LOG.info("Not able to find JSON config file for module {}", moduleDir);
          continue;
        }

        for (File configFile : jsonFiles) {
          String name = configFile.getName();
          String provisionerName = name.substring(0, name.lastIndexOf('.'));
          try (Reader reader = Files.newReader(configFile, Charsets.UTF_8)) {
            results.put(provisionerName, GSON.fromJson(new JsonReader(reader), ProvisionerConfig.class));
          } catch (Exception e) {
            LOG.warn("Exception reading JSON config file for provisioner {}. Ignoring file.", provisionerName, e);
          }
        }
      }
    }

    ClassLoader cl = ClassLoader.getSystemClassLoader();
    Set<String> remainingProvisioners = new HashSet<>(provisioners);
    remainingProvisioners.removeAll(results.keySet());
    remainingProvisioners.remove(NativeProvisioner.SPEC.getName());

    for (String provisionerName : remainingProvisioners) {
      String name = provisionerName.concat(".json");
      try (InputStreamReader reader = new InputStreamReader(cl.getResourceAsStream(name))) {
        results.put(provisionerName, GSON.fromJson(new JsonReader(reader), ProvisionerConfig.class));
      } catch (Exception e) {
        LOG.warn("Exception reading JSON config file for provisioner {}. Ignoring file.", provisionerName, e);
      }
    }

    return Collections.unmodifiableMap(results);
  }
}
