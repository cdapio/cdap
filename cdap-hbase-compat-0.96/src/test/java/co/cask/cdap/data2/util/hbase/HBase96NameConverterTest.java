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

package co.cask.cdap.data2.util.hbase;

import org.junit.Assert;
import org.junit.Test;

public class HBase96NameConverterTest {
  @Test
  public void testGetSysConfigTablePrefix() throws Exception {
    HBase96NameConverter hBaseNameConversionUtil = new HBase96NameConverter();
    Assert.assertEquals("cdap_system:", hBaseNameConversionUtil.getSysConfigTablePrefix("cdap_user:some_table"));
    Assert.assertEquals("cdap_system:", hBaseNameConversionUtil.getSysConfigTablePrefix("cdap.table_in_default_ns"));
  }
}
