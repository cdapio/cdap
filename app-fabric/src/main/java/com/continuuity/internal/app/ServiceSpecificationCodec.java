package com.continuuity.internal.app;

import com.continuuity.api.service.ServiceSpecification;
import com.continuuity.internal.service.DefaultServiceSpecification;
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
