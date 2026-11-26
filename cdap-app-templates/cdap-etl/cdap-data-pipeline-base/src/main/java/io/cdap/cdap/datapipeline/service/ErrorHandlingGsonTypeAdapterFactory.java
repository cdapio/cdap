/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.datapipeline.service;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;

/*
 * TypeAdapter factory for GSON which will skip any fields which have a parsing error
 */
public class ErrorHandlingGsonTypeAdapterFactory implements TypeAdapterFactory {
  public final <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
    TypeAdapter<T> delegate = gson.getDelegateAdapter(this, type);

    return new TypeAdapter<T>() {
      @Override
      public void write(JsonWriter writer, T value) throws IOException {
        delegate.write(writer, value);
      }

      @Override
      public T read(JsonReader reader) throws IOException {
        try {
          return delegate.read(reader);
        } catch (Exception e) {
          reader.skipValue();
          return null;
        }
      }
    };
  }
}
