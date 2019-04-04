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

package io.cdap.cdap.data2.dataset2.lib.kv;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 *
 */
public class LevelDBKVTableTest extends NoTxKeyValueTableTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static LevelDBTableService levelDBTableService;

  @BeforeClass
  public static void init() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    conf.set(Constants.CFG_DATA_LEVELDB_DIR, tmpFolder.newFolder().getAbsolutePath());
    levelDBTableService = new LevelDBTableService();
    levelDBTableService.setConfiguration(conf);
  }

  @After
  public void clear() {
    levelDBTableService.clearTables();
  }

  @Override
  protected DatasetDefinition<? extends NoTxKeyValueTable, ? extends DatasetAdmin> getDefinition() throws IOException {
    final String dataDir = tmpFolder.newFolder().getAbsolutePath();
    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        CConfiguration conf = CConfiguration.create();
        conf.set(Constants.CFG_DATA_LEVELDB_DIR, dataDir);
        bind(CConfiguration.class).toInstance(conf);
        bind(LevelDBTableService.class).toInstance(levelDBTableService);
      }
    });

    LevelDBKVTableDefinition def = new LevelDBKVTableDefinition("foo");
    injector.injectMembers(def);
    return def;
  }
}
