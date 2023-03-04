/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.proto;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.Map;

/**
 * A {@link TypeAdapterFactory} to serialize and deserialize {@link NamespaceConfig} as map.
 */
public class NamespaceConfigCodec implements TypeAdapterFactory {

  public static final TypeToken<Map<String, String>> MAP_TYPE_TOKEN = new TypeToken<Map<String, String>>() {
  };

  @Override
  public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
    if (!NamespaceConfig.class.equals(type.getRawType())) {
      return null;
    }

    TypeAdapter<Map<String, String>> mapTypeAdapter = gson.getAdapter(MAP_TYPE_TOKEN);

    return new TypeAdapter<T>() {
      @Override
      public void write(JsonWriter out, T value) throws IOException {
        if (value == null) {
          out.nullValue();
        } else {
          mapTypeAdapter.write(out, ((NamespaceConfig) value).getConfigs());
        }
      }

      @Override
      public T read(JsonReader in) throws IOException {
        Map<String, String> configs = mapTypeAdapter.read(in);
        if (configs == null) {
          return null;
        }
        //noinspection unchecked
        return (T) new NamespaceConfig(configs);
      }
    };
  }
}
