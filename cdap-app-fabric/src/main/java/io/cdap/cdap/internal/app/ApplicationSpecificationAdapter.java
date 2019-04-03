/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package co.cask.cdap.internal.app;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.customaction.CustomActionSpecification;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.schedule.Trigger;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.api.workflow.ConditionSpecification;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.internal.app.runtime.schedule.constraint.ConstraintCodec;
import co.cask.cdap.internal.app.runtime.schedule.trigger.SatisfiableTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TriggerCodec;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.codec.BasicThrowableCodec;
import co.cask.cdap.proto.codec.ConditionSpecificationCodec;
import co.cask.cdap.proto.codec.CustomActionSpecificationCodec;
import co.cask.cdap.proto.codec.MapReduceSpecificationCodec;
import co.cask.cdap.proto.codec.SparkSpecificationCodec;
import co.cask.cdap.proto.codec.WorkerSpecificationCodec;
import co.cask.cdap.proto.codec.WorkflowNodeCodec;
import co.cask.cdap.proto.codec.WorkflowSpecificationCodec;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.SortedMap;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Helper class to encoded/decode {@link ApplicationSpecification} to/from json.
 */
@NotThreadSafe
public final class ApplicationSpecificationAdapter {

  private final Gson gson;

  public static ApplicationSpecificationAdapter create() {
    return new ApplicationSpecificationAdapter(addTypeAdapters(new GsonBuilder()).create());
  }

  public static GsonBuilder addTypeAdapters(GsonBuilder builder) {
    return builder
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .registerTypeAdapter(ApplicationSpecification.class, new ApplicationSpecificationCodec())
      .registerTypeAdapter(MapReduceSpecification.class, new MapReduceSpecificationCodec())
      .registerTypeAdapter(SparkSpecification.class, new SparkSpecificationCodec())
      .registerTypeAdapter(WorkflowSpecification.class, new WorkflowSpecificationCodec())
      .registerTypeAdapter(WorkflowNode.class, new WorkflowNodeCodec())
      .registerTypeAdapter(CustomActionSpecification.class, new CustomActionSpecificationCodec())
      .registerTypeAdapter(ConditionSpecification.class, new ConditionSpecificationCodec())
      .registerTypeAdapter(ServiceSpecification.class, new ServiceSpecificationCodec())
      .registerTypeAdapter(WorkerSpecification.class, new WorkerSpecificationCodec())
      .registerTypeAdapter(BasicThrowable.class, new BasicThrowableCodec())
      .registerTypeAdapter(Trigger.class, new TriggerCodec())
      .registerTypeAdapter(SatisfiableTrigger.class, new TriggerCodec())
      .registerTypeAdapter(Constraint.class, new ConstraintCodec())
      .registerTypeAdapterFactory(new AppSpecTypeAdapterFactory());
  }

  public String toJson(ApplicationSpecification appSpec) {
    StringBuilder builder = new StringBuilder();
    toJson(appSpec, builder);
    return builder.toString();
  }

  public void toJson(ApplicationSpecification appSpec, Appendable appendable) {
    gson.toJson(appSpec, ApplicationSpecification.class, appendable);
  }

  public ApplicationSpecification fromJson(String json) {
    return gson.fromJson(json, ApplicationSpecification.class);
  }

  public ApplicationSpecification fromJson(Reader reader) throws IOException {
    try {
      return gson.fromJson(reader, ApplicationSpecification.class);
    } catch (JsonParseException e) {
      throw new IOException(e);
    }
  }

  private ApplicationSpecificationAdapter(Gson gson) {
    this.gson = gson;
  }

  private static final class AppSpecTypeAdapterFactory implements TypeAdapterFactory {

    @SuppressWarnings("unchecked")
    @Override
    public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
      Class<?> rawType = type.getRawType();
      // note: we want ordered maps to remain ordered
      if (!Map.class.isAssignableFrom(rawType) ||
        SortedMap.class.isAssignableFrom(rawType)) {
        return null;
      }
      // For non-parameterized Map, use the default TypeAdapter
      if (!(type.getType() instanceof ParameterizedType)) {
        return null;
      }

      Type[] typeArgs = ((ParameterizedType) type.getType()).getActualTypeArguments();
      TypeToken<?> keyType = TypeToken.get(typeArgs[0]);
      TypeToken<?> valueType = TypeToken.get(typeArgs[1]);
      if (keyType.getRawType() != String.class) {
        return null;
      }
      return (TypeAdapter<T>) mapAdapter(gson, valueType);
    }

    private <V> TypeAdapter<Map<String, V>> mapAdapter(Gson gson, TypeToken<V> valueType) {
      final TypeAdapter<V> valueAdapter = gson.getAdapter(valueType);

      return new TypeAdapter<Map<String, V>>() {
        @Override
        public void write(JsonWriter writer, Map<String, V> map) throws IOException {
          if (map == null) {
            writer.nullValue();
            return;
          }
          writer.beginObject();
          for (Map.Entry<String, V> entry : map.entrySet()) {
            writer.name(entry.getKey());
            valueAdapter.write(writer, entry.getValue());
          }
          writer.endObject();
        }

        @Override
        public Map<String, V> read(JsonReader reader) throws IOException {
          if (reader.peek() == JsonToken.NULL) {
            reader.nextNull();
            return null;
          }
          if (reader.peek() != JsonToken.BEGIN_OBJECT) {
            return null;
          }
          Map<String, V> map = Maps.newHashMap();
          reader.beginObject();
          while (reader.peek() != JsonToken.END_OBJECT) {
            map.put(reader.nextName(), valueAdapter.read(reader));
          }
          reader.endObject();
          return map;
        }
      };
    }
  }
}
