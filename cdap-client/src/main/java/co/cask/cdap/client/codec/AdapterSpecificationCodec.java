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

package co.cask.cdap.client.codec;

import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.templates.AdapterSpecification;
import co.cask.cdap.proto.codec.AbstractSpecificationCodec;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;
import javax.annotation.Nullable;

/**
 * Codec for {@link AdapterSpecification}.
 */
public class AdapterSpecificationCodec extends AbstractSpecificationCodec<AdapterSpecification>  {

  @Override
  public AdapterSpecification deserialize(JsonElement json, Type typeOfT,
                                          final JsonDeserializationContext context) throws JsonParseException {
    final String template = json.getAsJsonObject().get("template").getAsString();
    final String name = json.getAsJsonObject().get("name").getAsString();
    final String description = json.getAsJsonObject().has("description") ?
      json.getAsJsonObject().get("description").getAsString() : null;
    final JsonElement config = json.getAsJsonObject().get("config");
    final ScheduleSpecification scheduleSpec = json.getAsJsonObject().has("scheduleSpec") ?
      context.<ScheduleSpecification>deserialize(
        json.getAsJsonObject().get("scheduleSpec"), ScheduleSpecification.class) : null;

    return new AdapterSpecification() {
      @Override
      public String getTemplate() {
        return template;
      }

      @Override
      public String getName() {
        return name;
      }

      @Nullable
      @Override
      public String getDescription() {
        return description;
      }

      @Nullable
      @Override
      public String getConfigString() {
        return config.toString();
      }

      @Nullable
      @Override
      public <T> T getConfig(Type configType) {
        return context.deserialize(config, configType);
      }

      @Nullable
      @Override
      public ScheduleSpecification getScheduleSpecification() {
        return scheduleSpec;
      }
    };
  }

  @Override
  public JsonElement serialize(AdapterSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    return context.serialize(src);
  }
}
