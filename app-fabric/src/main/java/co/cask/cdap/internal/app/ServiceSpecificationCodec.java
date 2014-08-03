/*
 * Copyright 2014 Cask, Inc.
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

import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.internal.service.DefaultServiceSpecification;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.internal.json.TwillSpecificationAdapter;

import java.lang.reflect.Type;

/**
 * Codec to serialize and serialize {@link ServiceSpecification}
 */
public class ServiceSpecificationCodec extends AbstractSpecificationCodec<ServiceSpecification>  {

  private final TwillSpecificationAdapter adapter;

  public ServiceSpecificationCodec() {
    adapter = TwillSpecificationAdapter.create();
  }

  @Override
  public ServiceSpecification deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject jsonObj = (JsonObject) json;
    String className = jsonObj.get("classname").getAsString();
    TwillSpecification spec = adapter.fromJson(jsonObj.get("spec").getAsString());
    return new DefaultServiceSpecification(className, spec);
  }

  @Override
  public JsonElement serialize(ServiceSpecification spec, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject object = new JsonObject();
    object.addProperty("spec", adapter.toJson(spec));
    object.addProperty("classname", spec.getClassName());
    return object;
  }
}
