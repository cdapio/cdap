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

package io.cdap.cdap.data2.util.hbase;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.spi.hbase.HBaseDDLExecutor;
import io.cdap.cdap.spi.hbase.HBaseDDLExecutorContext;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 * Factory for providing {@link HBaseDDLExecutor}.
 */
public class HBaseDDLExecutorFactory extends HBaseVersionSpecificFactory<HBaseDDLExecutor> {

  private final HBaseDDLExecutorLoader hBaseDDLExecutorLoader;
  private final HBaseDDLExecutorContext context;
  private final String extensionDir;

  public HBaseDDLExecutorFactory(CConfiguration cConf, Configuration hConf) {
    super(cConf);
    this.extensionDir = cConf.get(Constants.HBaseDDLExecutor.EXTENSIONS_DIR);
    this.hBaseDDLExecutorLoader = new HBaseDDLExecutorLoader(
        extensionDir == null ? "" : extensionDir);
    this.context = new BasicHBaseDDLExecutorContext(cConf, hConf);
  }

  @Override
  public HBaseDDLExecutor get() {
    // Check if HBaseDDLExecutor extension is provided
    Map<String, HBaseDDLExecutor> extensions = hBaseDDLExecutorLoader.getAll();
    HBaseDDLExecutor executor;
    if (!extensions.isEmpty()) {
      // HBase DDL executor extension is provided.
      executor = extensions.values().iterator().next();
    } else {
      if (extensionDir != null) {
        // Extension directory is provided but the extension is not loaded
        throw new RuntimeException(
            String.format("HBaseDDLExecutor extension cannot be loaded from directory '%s'."
                + " Please make sure jar is available at that location with "
                + "appropriate permissions.", extensionDir));
      }
      // Return the version specific executor instance.
      executor = super.get();
    }

    executor.initialize(context);
    return executor;
  }

  @Override
  protected String getHBase10Classname() {
    return "io.cdap.cdap.data2.util.hbase.DefaultHBase10DDLExecutor";
  }

  @Override
  protected String getHBase10CDHClassname() {
    return "io.cdap.cdap.data2.util.hbase.DefaultHBase10CDHDDLExecutor";
  }

  @Override
  protected String getHBase11Classname() {
    return "io.cdap.cdap.data2.util.hbase.DefaultHBase11DDLExecutor";
  }

  @Override
  protected String getHBase10CHD550ClassName() {
    return "io.cdap.cdap.data2.util.hbase.DefaultHBase10CDH550DDLExecutor";
  }

  @Override
  protected String getHBase12CHD570ClassName() {
    return "io.cdap.cdap.data2.util.hbase.DefaultHBase12CDH570DDLExecutor";
  }
}
