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

package co.cask.cdap.common.io;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Type adapter that serializes enums in lowercase and deserializes enums in a case insensitive way.
 * Taken straight from the example in TypeAdapterFactory javadocs except deserializes in a case insenstivie way
 * instead of deserializing lowercase only.
 */
public class CaseInsensitiveEnumTypeAdapterFactory implements TypeAdapterFactory {
  public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
    @SuppressWarnings("unchecked")
    Class<T> rawType = (Class<T>) type.getRawType();
    if (!rawType.isEnum()) {
      return null;
    }

    final Map<String, T> lowercaseToConstant = new HashMap<>();
    for (T constant : rawType.getEnumConstants()) {
      lowercaseToConstant.put(toLowercase(constant), constant);
    }

    return new TypeAdapter<T>() {
      public void write(JsonWriter out, T value) throws IOException {
        if (value == null) {
          out.nullValue();
        } else {
          out.value(toLowercase(value));
        }
      }

      public T read(JsonReader reader) throws IOException {
        if (reader.peek() == JsonToken.NULL) {
          reader.nextNull();
          return null;
        } else {
          return lowercaseToConstant.get(toLowercase(reader.nextString()));
        }
      }
    };
  }

  private String toLowercase(Object o) {
    return o.toString().toLowerCase(Locale.US);
  }
}
