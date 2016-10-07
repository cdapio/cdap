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

import co.cask.cdap.messaging.store.PayloadTable;
import co.cask.cdap.messaging.store.PayloadTableTest;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * Tests for {@link LevelDBPayloadTable}.
 */
public class LevelDBPayloadTableTest extends PayloadTableTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final Iq80DBFactory LEVEL_DB_FACTORY = new Iq80DBFactory();

  @Override
  protected PayloadTable getPayloadTable() throws Exception {
    Options options = new Options()
      .createIfMissing(true)
      .errorIfExists(true);

    DB db = LEVEL_DB_FACTORY.open(new File(tmpFolder.newFolder(), "payload"), options);
    return new LevelDBPayloadTable(db);
  }
}
