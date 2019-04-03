/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.data2.util.hbase;

import co.cask.cdap.common.conf.CConfiguration;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * This class implements the client-side reading of the {@link CConfiguration} from HBase.
 */
public final class ClientCConfigurationReader extends ConfigurationReader implements CConfigurationReader {

  /**
   * Constructor from an HBase and CDAP configuration. This is useful for test cases.
   */
  public ClientCConfigurationReader(Configuration hConf, CConfiguration cConf) {
    super(hConf, cConf);
  }

  @Override
  public CConfiguration read() throws IOException {
    return read(ConfigurationReader.Type.DEFAULT);
  }
}
