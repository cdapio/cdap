/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Kryo serializer for Collections.UnmodifiableSortedMap
 */
public class UnmodifiableSortedMapSerializer extends Serializer<SortedMap> {
  @Override
  public void write(Kryo kryo, Output output, SortedMap map) {
    MapSerializer mapSerializer = new MapSerializer();
    mapSerializer.write(kryo, output, map);
  }

  @Override
  public SortedMap read(Kryo kryo, Input input, Class<SortedMap> aClass) {
    MapSerializer mapSerializer = new MapSerializer();
    Class<?> mapClass = TreeMap.class;
    Object object = mapSerializer.read(kryo, input, (Class<Map>) mapClass);

    return Collections.unmodifiableSortedMap((SortedMap) object);
  }
}
