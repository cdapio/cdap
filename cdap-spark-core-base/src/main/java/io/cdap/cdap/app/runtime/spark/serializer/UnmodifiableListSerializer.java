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
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
import org.apache.commons.collections.list.UnmodifiableList;
import org.apache.commons.collections.map.UnmodifiableMap;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Kryo serializer for Collections.UnmodifiableList
 */
public class UnmodifiableListSerializer extends Serializer<List> {

  @Override
  public void write(Kryo kryo, Output output, List objects) {
    CollectionSerializer collectionSerializer = new CollectionSerializer();
    collectionSerializer.write(kryo, output, objects);
  }

  @Override
  public List<Object> read(Kryo kryo, Input input, Class<List> aClass) {
    CollectionSerializer collectionSerializer = new CollectionSerializer();
    Class<?> listClass = LinkedList.class;
    Object object = collectionSerializer.read(kryo, input, (Class<Collection>) listClass);

    return Collections.unmodifiableList((List) object);
  }
}
