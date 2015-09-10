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

package co.cask.cdap.data.view;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.lang.reflect.Type;

/**
 * A {@link MetadataStoreDataset} with custom codec for serializing/deserialing objects.
 */
public class ViewMetadataStoreDataset extends MetadataStoreDataset {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();

  public ViewMetadataStoreDataset(Table table) {
    super(table);
  }

  @Override
  protected <T> byte[] serialize(T value) {
    return Bytes.toBytes(GSON.toJson(value));
  }

  @Override
  protected <T> T deserialize(byte[] serialized, Type typeOfT) {
    return GSON.fromJson(Bytes.toString(serialized), typeOfT);
  }
}
