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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractCloseableIterator;
import com.google.common.base.Throwables;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * LevelDB Table Scan Iterator that takes in optional startKey and stopKey.
 */
final class DBScanIterator extends AbstractCloseableIterator<Map.Entry<byte[], byte[]>> {
  private final DBIterator iterator;
  private final byte[] stopKey;

  private boolean closed = false;

  DBScanIterator(DB levelDB, @Nullable byte[] startKey, @Nullable final byte[] stopKey) {
    this.iterator = levelDB.iterator();
    this.stopKey = stopKey;
    if (startKey != null) {
      iterator.seek(startKey);
    } else {
      iterator.seekToFirst();
    }
  }

  @Override
  protected Map.Entry<byte[], byte[]> computeNext() {
    if (closed) {
      return endOfData();
    }
    if (iterator.hasNext()) {
      Map.Entry<byte[], byte[]> entry = iterator.next();
      if (!(stopKey != null && Bytes.compareTo(entry.getKey(), stopKey) >= 0)) {
        return entry;
      }
    }
    return endOfData();
  }

  @Override
  public void close() {
    try {
      iterator.close();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    } finally {
      endOfData();
      closed = true;
    }
  }
}
