/*
 * Copyright © 2014 Cask Data, Inc.
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
import com.google.inject.Inject;

/**
 * Factory for HBase version-specific {@link HBaseTableUtil} instances.
 */
public class HBaseTableUtilFactory extends HBaseVersionSpecificFactory<HBaseTableUtil> {

  private final CConfiguration cConf;

  @Inject
  public HBaseTableUtilFactory(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Override
  protected HBaseTableUtil createInstance(String className) throws ClassNotFoundException {
    HBaseTableUtil hBaseTableUtil = super.createInstance(className);
    hBaseTableUtil.setCConf(cConf);
    return hBaseTableUtil;
  }

  public static Class<? extends HBaseTableUtil> getHBaseTableUtilClass() {
    // Since we only need the class name, it is fine to have a null CConfiguration, since we do not use the
    // tableUtil instance
    return new HBaseTableUtilFactory(null).get().getClass();
  }

  @Override
  protected String getHBase94Classname() {
    return "co.cask.cdap.data2.util.hbase.HBase94TableUtil";
  }

  @Override
  protected String getHBase96Classname() {
    return "co.cask.cdap.data2.util.hbase.HBase96TableUtil";
  }

  @Override
  protected String getHBase98Classname() {
    return "co.cask.cdap.data2.util.hbase.HBase98TableUtil";
  }
}
