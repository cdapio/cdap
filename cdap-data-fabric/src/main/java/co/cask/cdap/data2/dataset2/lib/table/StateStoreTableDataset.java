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
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRecord;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Dataset that manages a table of Program State.
 */
public class StateStoreTableDataset extends AbstractDataset implements StateStoreTable {
  private static final Logger LOG = LoggerFactory.getLogger(StateStoreTableDataset.class);
  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Gson GSON = new Gson();
  private final KeyValueTable table;

  public StateStoreTableDataset(DatasetSpecification spec, @EmbeddedDataset("states") KeyValueTable table) {
    super(spec.getName(), table);
    this.table = table;
  }

  @Override
  public Map<String, String> getState(ProgramRecord program) {
    try {
      byte[] state = table.read(generateStateStoreRowKey(program));
      if (state == null) {
        return Maps.newHashMap();
      } else {
        return GSON.fromJson(Bytes.toString(table.read(generateStateStoreRowKey(program))), STRING_MAP_TYPE);
      }
    } catch (Exception e) {
      LOG.debug("Get State failed for {}", program);
    }
    return Maps.newHashMap();
  }

  @Override
  public void saveState(ProgramRecord program, Map<String, String> state) {
    try {
      table.write(generateStateStoreRowKey(program), Bytes.toBytes(GSON.toJson(state)));
    } catch (Exception e) {
      LOG.debug("Set State failed for {}", program);
    }
  }

  @Override
  public void deleteState(ProgramRecord program) {
    try {
      table.delete(generateStateStoreRowKey(program));
    } catch (Exception e) {
      LOG.debug("Delete State failed for {}", program);
    }
  }

  @Override
  public void deleteState(Id.Application appId) {
    try {
      byte[] startRowKey = Bytes.toBytes(appId.getId() + ".");
      byte[] endRowKey = Bytes.stopKeyForPrefix(startRowKey);
      CloseableIterator<KeyValue<byte[], byte[]>> rows = table.scan(startRowKey, endRowKey);
      while (rows.hasNext()) {
        table.delete(rows.next().getKey());
      }
      rows.close();
    } catch (Exception e) {
      LOG.debug("Delete State failed for Application {}", appId);
    }
  }

  private byte[] generateStateStoreRowKey(ProgramRecord program) {
    return Bytes.toBytes(String.format("%s.%s.%s", program.getApp(), program.getType().getPrettyName(),
                                       program.getId()));
  }
}
