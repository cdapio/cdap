/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.test.XSlowTests;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.junit.experimental.categories.Category;

/**
 *
 */
@Category(XSlowTests.class)
public class HBase98TableUtilTest extends AbstractHBaseTableUtilTest {

  private final HTableNameConverter nameConverter = new HTable98NameConverter();

  @Override
  protected HBaseTableUtil getTableUtil() {
    HBase98TableUtil hBaseTableUtil = new HBase98TableUtil();
    hBaseTableUtil.setCConf(cConf);
    return hBaseTableUtil;
  }

  @Override
  protected HTableNameConverter getNameConverter() {
    return nameConverter;
  }

  @Override
  protected String getTableNameAsString(TableId tableId) {
    Preconditions.checkArgument(tableId != null, "TableId should not be null.");
    String tablePrefix = cConf.get(Constants.Dataset.TABLE_PREFIX);
    if (Constants.DEFAULT_NAMESPACE_ID.equals(tableId.getNamespace())) {
      return nameConverter.getHBaseTableName(tablePrefix, tableId);
    }
    return Joiner.on(':').join(nameConverter.toHBaseNamespace(tablePrefix, tableId.getNamespace()),
                               nameConverter.getHBaseTableName(tablePrefix, tableId));
  }

  @Override
  protected boolean namespacesSupported() {
    return true;
  }
}
