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

package co.cask.cdap.etl.api.lookup;

import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.etl.api.Lookup;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

/**
 * {@link Lookup} implementation for {@link KeyValueTable}.
 */
public class KeyValueTableLookup extends BaseKeyValueTableLookup<byte[]> {

  private final KeyValueTable table;

  public KeyValueTableLookup(KeyValueTable table) {
    this.table = table;
  }

  @Override
  public byte[] lookup(byte[] key) {
    return table.read(key);
  }

  @Override
  public Map<byte[], byte[]> lookup(byte[]... keys) {
    return table.readAll(keys);
  }

  @Override
  public byte[] lookup(String key) {
    return table.read(key);
  }

  @Override
  public Map<String, byte[]> lookup(String... keys) {
    return lookup(ImmutableSet.copyOf(keys));
  }

  @Override
  public Map<String, byte[]> lookup(Set<String> keys) {
    return fromBytes(table.readAll(toBytes(keys)), false);
  }
}
