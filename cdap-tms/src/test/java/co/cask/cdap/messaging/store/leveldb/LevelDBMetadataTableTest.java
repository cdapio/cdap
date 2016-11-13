/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.messaging.store.leveldb;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.messaging.store.MetadataTable;
import co.cask.cdap.messaging.store.MetadataTableTest;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link LevelDBMetadataTable}
 */
public class LevelDBMetadataTableTest extends MetadataTableTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static LevelDBTableService service;

  @BeforeClass
  public static void init() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.CFG_DATA_LEVELDB_DIR, tmpFolder.newFolder().getAbsolutePath());
    service = new LevelDBTableService();
    service.setConfiguration(conf);
  }

  @Override
  protected MetadataTable getTable() throws Exception {
    service.ensureTableExists("metadata");
    return new LevelDBMetadataTable(service, "metadata");
  }
}
