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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.data2.dataset2.lib.table.ObjectStoreDataset;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.internal.io.TypeRepresentation;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;

/**
 * A simple data set <i>extending</i> ObjectStore, used by ObjectStoreTest.testSubclass().
 */
public class IntegerStore extends ObjectStoreDataset<Integer> {

  public IntegerStore(String name, KeyValueTable kvTable) throws UnsupportedTypeException {
    super(name, kvTable, new TypeRepresentation(Integer.class),
          new ReflectionSchemaGenerator().generate(Integer.class));
  }

  public void write(int key, Integer value) throws Exception {
    super.write(Bytes.toBytes(key), value);
  }

  public Integer read(int key) throws Exception {
    return super.read(Bytes.toBytes(key));
  }

  public void delete(int key) {
    super.delete(Bytes.toBytes(key));
  }
}
