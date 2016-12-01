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

package co.cask.cdap.internal.app.store.preview;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableCore;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.NoTxTable;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Throwables;
import org.iq80.leveldb.DB;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 *  Non transactional table implementation for LevelDB.
 */
public class LevelDBNoTxPreviewTable implements NoTxTable {

  private static final DatasetId PREVIEW_TABLE_ID = NamespaceId.SYSTEM.dataset("preview.table");
  private static final byte[] TRACER = Bytes.toBytes("t");
  private static final byte[] PROPERTY = Bytes.toBytes("p");
  private static final byte[] VALUE = Bytes.toBytes("v");
  private static final byte[][] COLUMNS = new byte[][] {TRACER, PROPERTY, VALUE };

  private final LevelDBTableCore core;

  LevelDBNoTxPreviewTable(LevelDBTableService service) {
    try {
      this.core = new LevelDBTableCore(PREVIEW_TABLE_ID.getDataset(), service);
    } catch (IOException e) {
      throw new RuntimeException("Error creating preview table", e);
    }
  }

  @Override
  public void put(byte[] row, byte[] column, byte[] value, long version) throws IOException {
    core.put(row, column, value, version);
  }

  @Override
  public Scanner scan(@Nullable byte[] startRowKey, @Nullable byte[] stopRowKey) throws IOException {
    return core.scan(startRowKey, stopRowKey, null, null, null);
  }

  @Override
  public void close() throws IOException {
    // no-op
  }
}
