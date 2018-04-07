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

package co.cask.cdap.internal.provision;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.DirUtils;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;

/**
 * Default implementation of the ProvsionerConfigProvider. It expects a json file for from each module dir and
 * expects a "configuration-groups" in the json file
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
          LOG.info("Not able to find configuration groups file for module {}", moduleDir);
          continue;
        }

        for (File configFile : jsonFiles) {
          String name = configFile.getName();
          String provisionerName = name.substring(0, name.lastIndexOf('.'));
          try (Reader reader = Files.newReader(configFile, Charsets.UTF_8)) {
            results.put(provisionerName, GSON.fromJson(new JsonReader(reader), ProvisionerConfig.class));
          } catch (Exception e) {
            LOG.warn("Exception reading configuration groups file for provisioner {}. Ignoring file.",
                     provisionerName, e);
          }
        }
      }
    }
    return Collections.unmodifiableMap(results);
  }
}
