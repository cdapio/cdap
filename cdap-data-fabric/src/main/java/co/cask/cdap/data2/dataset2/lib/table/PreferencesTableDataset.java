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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.OrderedTable;
import co.cask.cdap.proto.ProgramRecord;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Dataset that manages a table of Program Preferences.
 */
public class PreferencesTableDataset extends AbstractDataset implements PreferencesTable {
  private static final Logger LOG = LoggerFactory.getLogger(PreferencesTableDataset.class);
  private static final String STATE_STORE_ROWPREFIX = "statestore";
  private final OrderedTable table;

  public PreferencesTableDataset(DatasetSpecification spec, @EmbeddedDataset("prefs") OrderedTable table) {
    super(spec.getName(), table);
    this.table = table;
  }

  @Override
  public String getState(ProgramRecord program, String key) {
    try {
      return Bytes.toString(table.get(generateStateStoreRowKey(program), Bytes.toBytes(key)));
    } catch (Exception e) {
      LOG.debug("Get Note failed for {} : Key {}", program, key);
    }
    return null;
  }

  @Override
  public void saveState(ProgramRecord program, String key, String value) {
    try {
      table.put(generateStateStoreRowKey(program), Bytes.toBytes(key), Bytes.toBytes(value));
    } catch (Exception e) {
      LOG.debug("Set Note failed for {} : Key {} : Value {}", program, key, value);
    }
  }

  @Override
  public Map<String, String> getState(ProgramRecord program) {
    Map<String, String> notes = Maps.newHashMap();
    try {
      for (Map.Entry<byte[], byte[]> entry : table.get(generateStateStoreRowKey(program)).entrySet()) {
        notes.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
      }
      return notes;
    } catch (Exception e) {
      LOG.debug("Get Notes failed for {}", program);
    }
    return null;
  }

  @Override
  public void saveState(ProgramRecord program, Map<String, String> state) {
    byte[][] columns = new byte[state.size()][];
    byte[][] values = new byte[state.size()][];
    int i = 0;
    try {
      for (Map.Entry<String, String> columnValue : state.entrySet()) {
        columns[i] = Bytes.toBytes(columnValue.getKey());
        values[i] = Bytes.toBytes(columnValue.getValue());
        i++;
      }
      table.put(generateStateStoreRowKey(program), columns, values);
    } catch (Exception e) {
      LOG.debug("Set Notes failed for {}", program);
    }
  }

  @Override
  public void deleteState(ProgramRecord program) {
    try {
      table.delete(generateStateStoreRowKey(program));
    } catch (Exception e) {
      LOG.debug("Delete Notes failed for {}", program);
    }
  }

  private byte[] generateStateStoreRowKey(ProgramRecord program) {
    //Use default namespace until namespace is implemented.
    return Bytes.toBytes(String.format("%s.%s.%s.%s.%s", STATE_STORE_ROWPREFIX, "default", program.getApp(),
                                       program.getType().getPrettyName(), program.getId()));
  }
}
