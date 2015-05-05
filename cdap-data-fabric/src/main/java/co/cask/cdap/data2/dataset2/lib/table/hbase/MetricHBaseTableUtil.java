/*
 * Copyright 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table.hbase;

import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.data2.dataset2.lib.hbase.AbstractHBaseDataSetAdmin;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import org.apache.hadoop.hbase.HTableDescriptor;

import java.util.Map;

/**
 * Utility to determine Metric System's HBase table.
 */
public class MetricHBaseTableUtil {
  private final HBaseTableUtil tableUtil;

  public MetricHBaseTableUtil(HBaseTableUtil tableUtil) {
    this.tableUtil = tableUtil;
  }

  /**
   * Denotes version of Metric System's HBase table.
   */
  public static enum Version {
    VERSION_2_6_OR_LOWER,
    VERSION_2_7,
    VERSION_2_8_OR_HIGHER
  }

  public Version getVersion(HTableDescriptor tableDescriptor) {
    // 1) First, try to use cdap.version property on HBase table. 
    // 2) If cdap.version property is not available, then it is 2.7 or 2.6 & older.
    // 2a) If table has no increment handler coprocessor, it is 2.6 or older
    // 2b) If table has increment handler coprocessor, it is 2.7
    
    ProjectInfo.Version version = AbstractHBaseDataSetAdmin.getVersion(tableDescriptor);
    // note: major version is 0 if table doesn't have the cdap version property
    if (version.getMajor() > 0) {
      if (version.getMajor() < 2) {
        return Version.VERSION_2_6_OR_LOWER;
      }
      if (version.getMajor() == 2 && version.getMinor() <= 6) {
        return Version.VERSION_2_6_OR_LOWER;
      }
      if (version.getMajor() == 2 && version.getMinor() <= 7) {
        return Version.VERSION_2_7;
      }
      return Version.VERSION_2_8_OR_HIGHER;
    }

    Map<String, HBaseTableUtil.CoprocessorInfo> cpsInfo = HBaseTableUtil.getCoprocessorInfo(tableDescriptor);
    if (cpsInfo.containsKey(tableUtil.getIncrementHandlerClassForVersion().getName())) {
      // note: if the version is 2.8 or higher, it would have cdap.version property
      return Version.VERSION_2_7;
    }

    return Version.VERSION_2_6_OR_LOWER;
  }
}
