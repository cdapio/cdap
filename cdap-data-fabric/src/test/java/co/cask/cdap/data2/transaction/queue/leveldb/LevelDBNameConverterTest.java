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

package co.cask.cdap.data2.transaction.queue.leveldb;

import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBNameConverter;
import co.cask.cdap.data2.util.TableId;
import org.junit.Assert;
import org.junit.Test;

/**
 * tests converting level db table name to {@link co.cask.cdap.data2.util.TableId}
 */
public class LevelDBNameConverterTest {
  @Test
  public void testLevelDBNameConversion() throws Exception {
    Assert.assertEquals(TableId.from("default", "history.kv.table"),
                        LevelDBNameConverter.from("cdap_default.history.kv.table"));
    Assert.assertEquals(TableId.from("ns1", "frequent.test"),
                        LevelDBNameConverter.from("cdap_ns1.frequent.test"));

  }


  @Test(expected = IllegalArgumentException.class)
  public void testLevelDBNameConversionInvalidPrefix() throws Exception {
    Assert.assertEquals(TableId.from("default", "history.kv.table"),
                        LevelDBNameConverter.from("cdap.default.history.kv.table"));

  }

  @Test(expected = IllegalArgumentException.class)
  public void testLevelDBNameConversionMissingTableName() throws Exception {
    Assert.assertEquals(TableId.from("default", "history.kv.table"),
                        LevelDBNameConverter.from("cdap_default"));

  }
}
