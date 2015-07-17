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
package co.cask.cdap.etl.common;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.etl.api.Lookup;

import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class DefaultLookup implements Lookup {

  private final Transactional tx;

  public DefaultLookup(Transactional tx) {
    this.tx = tx;
  }

  @Override
  public String lookupKVString(String instance, String key) throws Exception {
    return Bytes.toString(lookupKVBytes(instance, Bytes.toBytes(key)));
  }

  @Override
  public byte[] lookupKVBytes(String instance, String key) throws Exception {
    return lookupKVBytes(instance, Bytes.toBytes(key));
  }

  @Override
  public byte[] lookupKVBytes(final String instance, final byte[] key) throws Exception {
    final AtomicReference<byte[]> result = new AtomicReference<>();
    tx.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        Dataset dataset = context.getDataset(instance);
        if (!(dataset instanceof KeyValueTable)) {
          throw new IllegalArgumentException(String.format("Dataset %s is not an KeyValueTable", instance));
        }

        KeyValueTable kv = (KeyValueTable) dataset;
        result.set(kv.read(key));
      }
    });
    return result.get();
  }
}
