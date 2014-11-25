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
public class PreferenceTableDataset extends AbstractDataset implements PreferenceTable {
  private static final Logger LOG = LoggerFactory.getLogger(PreferenceTableDataset.class);
  private final OrderedTable table;

  public PreferenceTableDataset(DatasetSpecification spec, @EmbeddedDataset("prefs") OrderedTable table) {
    super(spec.getName(), table);
    this.table = table;
  }

  @Override
  public String getNote(ProgramRecord program, String key) {
    try {
      return Bytes.toString(table.get(generateRowKey(program), Bytes.toBytes(key)));
    } catch (Exception e) {
      LOG.debug("Get Note failed for {} : Key {}", program, key);
    }
    return null;
  }

  @Override
  public void setNote(ProgramRecord program, String key, String value) {
    try {
      table.put(generateRowKey(program), Bytes.toBytes(key), Bytes.toBytes(value));
    } catch (Exception e) {
      LOG.debug("Set Note failed for {} : Key {} : Value {}", program, key, value);
    }
  }

  @Override
  public Map<String, String> getNotes(ProgramRecord program) {
    Map<String, String> notes = Maps.newHashMap();
    try {
      for (Map.Entry<byte[], byte[]> entry : table.get(generateRowKey(program)).entrySet()) {
        notes.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
      }
      return notes;
    } catch (Exception e) {
      LOG.debug("Get Notes failed for {}", program);
    }
    return null;
  }

  @Override
  public void setNotes(ProgramRecord program, Map<String, String> notes) {
    byte[][] columns = new byte[notes.size()][];
    byte[][] values = new byte[notes.size()][];
    int i = 0;
    try {
      for (Map.Entry<String, String> columnValue : notes.entrySet()) {
        columns[i] = Bytes.toBytes(columnValue.getKey());
        values[i] = Bytes.toBytes(columnValue.getValue());
        i++;
      }
      table.put(generateRowKey(program), columns, values);
    } catch (Exception e) {
      LOG.debug("Set Notes failed for {}", program);
    }
  }

  @Override
  public void deleteNotes(ProgramRecord program) {
    try {
      table.delete(generateRowKey(program));
    } catch (Exception e) {
      LOG.debug("Delete Notes failed for {}", program);
    }
  }

  private byte[] generateRowKey(ProgramRecord program) {
    //Use default namespace until namespace is implemented.
    return Bytes.toBytes(String.format("%s.%s.%s.%s", "default", program.getApp(), program.getType().getPrettyName(),
                                       program.getId()));
  }
}
