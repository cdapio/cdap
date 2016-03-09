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

package co.cask.cdap.etl.batch.spark;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Helpers for serializing and deserializing collections.
 */
final class Serializations {

  static <V> void serializeMap(Map<String, V> map, ObjectWriter<V> valueWriter, DataOutput output) throws IOException {
    output.writeInt(map.size());
    for (Map.Entry<String, V> entry : map.entrySet()) {
      output.writeUTF(entry.getKey());
      valueWriter.write(entry.getValue(), output);
    }
  }

  static <V> Map<String, V> deserializeMap(DataInput input, ObjectReader<V> valueReader) throws IOException {
    Map<String, V> map = new HashMap<>();
    int size = input.readInt();
    for (int i = 0; i < size; i++) {
      map.put(input.readUTF(), valueReader.read(input));
    }
    return map;
  }

  static ObjectReader<String> createStringObjectReader() {
    return new ObjectReader<String>() {
      @Override
      public String read(DataInput input) throws IOException {
        return input.readUTF();
      }
    };
  }

  static ObjectWriter<String> createStringObjectWriter() {
    return new ObjectWriter<String>() {
      @Override
      public void write(String object, DataOutput output) throws IOException {
        output.writeUTF(object);
      }
    };
  }

  static ObjectReader<Set<String>> createStringSetObjectReader() {
    return new ObjectReader<Set<String>>() {
      @Override
      public Set<String> read(DataInput input) throws IOException {
        int setSize = input.readInt();
        Set<String> outputs = new HashSet<>();
        while (setSize > 0) {
          outputs.add(input.readUTF());
          setSize--;
        }
        return outputs;
      }
    };
  }

  static ObjectWriter<Set<String>> createStringSetObjectWriter() {
    return new ObjectWriter<Set<String>>() {
      @Override
      public void write(Set<String> object, DataOutput output) throws IOException {
        output.writeInt(object.size());
        for (String val : object) {
          output.writeUTF(val);
        }
      }
    };
  }

  interface ObjectWriter<T> {
    void write(T object, DataOutput output) throws IOException;
  }

  interface ObjectReader<T> {
    T read(DataInput input) throws IOException;
  }

  private Serializations() {
    // no-op
  }
}
