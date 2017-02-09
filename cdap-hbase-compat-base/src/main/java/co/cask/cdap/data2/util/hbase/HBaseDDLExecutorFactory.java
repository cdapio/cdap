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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.spi.hbase.HBaseDDLExecutor;
import co.cask.cdap.spi.hbase.HBaseDDLExecutorContext;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/**
 * Factory for providing {@link HBaseDDLExecutor}.
 */
public class HBaseDDLExecutorFactory extends HBaseVersionSpecificFactory<HBaseDDLExecutor> {

  private final HBaseDDLExecutorLoader hBaseDDLExecutorLoader;
  private final HBaseDDLExecutorContext context;

  public HBaseDDLExecutorFactory(CConfiguration cConf, Configuration hConf) {
    this.hBaseDDLExecutorLoader = new HBaseDDLExecutorLoader(cConf.get(Constants.HBaseDDLExecutor.EXTENSIONS_DIR, ""));
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
      // HBase DDL executor extension is not provided. Return the version specific executor instance.
      executor = super.get();
    }

    executor.initialize(context);
    return executor;
  }

  @Override
  protected String getHBase96Classname() {
    return "co.cask.cdap.data2.util.hbase.DefaultHBase96DDLExecutor";
  }

  @Override
  protected String getHBase98Classname() {
    return "co.cask.cdap.data2.util.hbase.DefaultHBase98DDLExecutor";
  }

  @Override
  protected String getHBase10Classname() {
    return "co.cask.cdap.data2.util.hbase.DefaultHBase10DDLExecutor";
  }

  @Override
  protected String getHBase10CDHClassname() {
    return "co.cask.cdap.data2.util.hbase.DefaultHBase10CDHDDLExecutor";
  }

  @Override
  protected String getHBase11Classname() {
    return "co.cask.cdap.data2.util.hbase.DefaultHBase11DDLExecutor";
  }

  @Override
  protected String getHBase10CHD550ClassName() {
    return "co.cask.cdap.data2.util.hbase.DefaultHBase10CDH550DDLExecutor";
  }

  @Override
  protected String getHBase12CHD570ClassName() {
    return "co.cask.cdap.data2.util.hbase.DefaultHBase12CDH570DDLExecutor";
  }
}
