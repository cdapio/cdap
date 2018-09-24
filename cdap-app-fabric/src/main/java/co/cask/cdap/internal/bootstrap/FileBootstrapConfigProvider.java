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
 *
 */

package co.cask.cdap.internal.bootstrap;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

/**
 * Reads the bootstrap config from a file. If there was an error reading the file, no bootstrapping will be
 * performed. If the file does not exist, the default bootstrap steps will be executed.
 */
public class FileBootstrapConfigProvider implements BootstrapConfigProvider {
  private static final Logger LOG = LoggerFactory.getLogger(FileBootstrapConfigProvider.class);
  private static final Gson GSON = new Gson();
  private final File bootstrapFile;

  @Inject
  FileBootstrapConfigProvider(CConfiguration cConf) {
    this.bootstrapFile = new File(cConf.get(Constants.BOOTSTRAP_FILE));
  }

  @Override
  public BootstrapConfig getConfig() {
    try {
      try (Reader reader = new FileReader(bootstrapFile)) {
        return GSON.fromJson(reader, BootstrapConfig.class);
      }
    } catch (FileNotFoundException e) {
      LOG.info("Bootstrap file {} does not exist. Default bootstrapping will be done.",
               bootstrapFile.getAbsolutePath());
      return BootstrapConfig.DEFAULT;
    } catch (JsonParseException e) {
      LOG.warn("Could not parse bootstrap file {}. No bootstrapping will be done.",
               bootstrapFile.getAbsolutePath(), e);
      return BootstrapConfig.EMPTY;
    } catch (IOException e) {
      LOG.warn("Could not read bootstrap file {}. No bootstrapping will be done.",
               bootstrapFile.getAbsolutePath(), e);
      return BootstrapConfig.EMPTY;
    }
  }
}
