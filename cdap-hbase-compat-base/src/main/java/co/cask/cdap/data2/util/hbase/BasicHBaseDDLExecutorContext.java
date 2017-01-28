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
import co.cask.cdap.spi.hbase.HBaseDDLExecutorContext;
import org.apache.hadoop.conf.Configuration;

import java.util.Collections;
import java.util.Map;

/**
 * Basic implementation of the {@link HBaseDDLExecutorContext}.
 */
class BasicHBaseDDLExecutorContext implements HBaseDDLExecutorContext {
  private final Configuration hConf;
  private final Map<String, String> properties;
  private static final String PROPERTY_PREFIX = "cdap.hbase.spi.";

  BasicHBaseDDLExecutorContext(CConfiguration cConf, Configuration hConf) {
    this.hConf = hConf;
    this.properties = Collections.unmodifiableMap(cConf.getPropsWithPrefix(PROPERTY_PREFIX));
  }

  @Override
  public <T> T getConfiguration() {
    return (T) hConf;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }
}
