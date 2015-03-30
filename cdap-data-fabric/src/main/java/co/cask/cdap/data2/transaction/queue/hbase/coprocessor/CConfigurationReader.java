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

package co.cask.cdap.data2.transaction.queue.hbase.coprocessor;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.util.hbase.ConfigurationTable;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * This class helps abstract out reading of CConfiguration from {@link ConfigurationTable} from different HBase version.
 */
public final class CConfigurationReader {

  private final ConfigurationTable configTable;
  private final String configTablePrefix;

  public CConfigurationReader(Configuration hConf, String configTablePrefix) {
    this.configTable = new ConfigurationTable(hConf);
    this.configTablePrefix = configTablePrefix;
  }

  public CConfiguration read() throws IOException {
    return configTable.read(ConfigurationTable.Type.DEFAULT, configTablePrefix);
  }
}
