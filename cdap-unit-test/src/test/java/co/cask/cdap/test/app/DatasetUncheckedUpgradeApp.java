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

package co.cask.cdap.test.app;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import com.clearspring.analytics.util.Preconditions;
import com.google.common.base.Objects;
import com.google.gson.Gson;

/**
 *
 */
public class DatasetUncheckedUpgradeApp extends AbstractApplication {
  public static final String NAME = "DatasetUncheckedUpgradeApp";
  public static final String DATASET_NAME = "dataset";
  private static final Gson GSON = new Gson();

  @Override
  public void configure() {
    setName(NAME);
    createDataset(DATASET_NAME, RecordDataset.class,
                  DatasetProperties.builder().add("recordClassName", getRecordClass().getName()).build());
    addService("NoOpService", new NoOpHandler());
  }

  public static final class RecordDataset extends AbstractDataset {
    private final KeyValueTable table;
    private final Class recordClass;

    public RecordDataset(DatasetSpecification spec,
                         @EmbeddedDataset("table") KeyValueTable table) throws ClassNotFoundException {
      super(spec.getName(), table);
      this.table = table;
      this.recordClass = Class.forName(spec.getProperty("recordClassName"));
    }

    public Object getRecord(String key) {
      byte[] serializedRecord = table.read(key);
      if (serializedRecord == null) {
        return null;
      }
      return GSON.fromJson(Bytes.toString(table.read(key)), recordClass);
    }

    public void writeRecord(String key, Object object) {
      Preconditions.checkArgument(recordClass.isInstance(object));
      table.write(key, GSON.toJson(object, recordClass));
    }
  }

  protected Class<?> getRecordClass() {
    return Record.class;
  }

  public static final class Record {
    private final String id;
    private final String firstName;
    private final String lastName;

    public Record(String id, String firstName, String lastName) {
      this.id = id;
      this.firstName = firstName;
      this.lastName = lastName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Record that = (Record) o;

      return Objects.equal(this.id, that.id) &&
        Objects.equal(this.firstName, that.firstName) &&
        Objects.equal(this.lastName, that.lastName);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(id, firstName, lastName);
    }
  }

  public static final class NoOpHandler extends AbstractHttpServiceHandler {

  }

}
