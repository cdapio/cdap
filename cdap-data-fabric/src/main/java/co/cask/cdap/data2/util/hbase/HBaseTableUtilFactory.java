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

/**
 * Factory for HBase version-specific {@link HBaseTableUtil} instances.
 */
public class HBaseTableUtilFactory {
  private final HBaseVersionSpecificFactory<HBaseTableUtil> delegate;

  public HBaseTableUtilFactory() {
    delegate = new HBaseVersionSpecificFactory<HBaseTableUtil>() {
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
    };
  }

  // There are a few places where we only need the class of the HBaseTableUtil, and so it does not need to be configured
  // with a CConfiguration
  public Class<? extends HBaseTableUtil> getHBaseTableUtilClass() {
    return delegate.get().getClass();
  }

  public HBaseTableUtil get(CConfiguration cConf) {
    HBaseTableUtil hBaseTableUtil = delegate.get();
    hBaseTableUtil.setCConf(cConf);
    return hBaseTableUtil;
  }

}
