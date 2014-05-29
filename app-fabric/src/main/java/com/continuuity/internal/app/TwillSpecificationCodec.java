package com.continuuity.internal.app;

/**
 *
 */
import com.google.common.reflect.TypeToken;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.twill.api.EventHandlerSpecification;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.internal.DefaultEventHandlerSpecification;
import org.apache.twill.internal.DefaultTwillSpecification;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of gson serializer/deserializer {@link org.apache.twill.api.TwillSpecification}.
 */
final class TwillSpecificationCodec implements JsonSerializer<TwillSpecification>,
  JsonDeserializer<TwillSpecification> {

  @Override
  public JsonElement serialize(TwillSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();
    json.addProperty("name", src.getName());
    json.add("runnables", context.serialize(src.getRunnables(),
                                            new TypeToken<Map<String, RuntimeSpecification>>() { }.getType()));
    json.add("orders", context.serialize(src.getOrders(),
                                         new TypeToken<List<TwillSpecification.Order>>() { }.getType()));
    EventHandlerSpecification eventHandler = src.getEventHandler();
    if (eventHandler != null) {
      json.add("handler", context.serialize(eventHandler, EventHandlerSpecification.class));
    }

    return json;
  }

  @Override
  public TwillSpecification deserialize(JsonElement json, Type typeOfT,
                                        JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();

    String name = jsonObj.get("name").getAsString();
    Map<String, RuntimeSpecification> runnables = context.deserialize(
      jsonObj.get("runnables"), new TypeToken<Map<String, RuntimeSpecification>>() { }.getType());
    List<TwillSpecification.Order> orders = context.deserialize(
      jsonObj.get("orders"), new TypeToken<List<TwillSpecification.Order>>() { }.getType());

    JsonElement handler = jsonObj.get("handler");
    EventHandlerSpecification eventHandler = null;
    if (handler != null && !handler.isJsonNull()) {
      eventHandler = context.deserialize(handler, EventHandlerSpecification.class);
    }

    return new DefaultTwillSpecification(name, runnables, orders, eventHandler);
  }

  static final class TwillSpecificationOrderCoder implements JsonSerializer<TwillSpecification.Order>,
    JsonDeserializer<TwillSpecification.Order> {

    @Override
    public JsonElement serialize(TwillSpecification.Order src, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject json = new JsonObject();
      json.add("names", context.serialize(src.getNames(), new TypeToken<Set<String>>() { }.getType()));
      json.addProperty("type", src.getType().name());
      return json;
    }

    @Override
    public TwillSpecification.Order deserialize(JsonElement json, Type typeOfT,
                                                JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObj = json.getAsJsonObject();

      Set<String> names = context.deserialize(jsonObj.get("names"), new TypeToken<Set<String>>() { }.getType());
      TwillSpecification.Order.Type type = TwillSpecification.Order.Type.valueOf(jsonObj.get("type").getAsString());

      return new DefaultTwillSpecification.DefaultOrder(names, type);
    }
  }

  static final class EventHandlerSpecificationCoder implements JsonSerializer<EventHandlerSpecification>,
    JsonDeserializer<EventHandlerSpecification> {

    @Override
    public JsonElement serialize(EventHandlerSpecification src, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject json = new JsonObject();
      json.addProperty("classname", src.getClassName());
      json.add("configs", context.serialize(src.getConfigs(), new TypeToken<Map<String, String>>() { }.getType()));
      return json;
    }

    @Override
    public EventHandlerSpecification deserialize(JsonElement json, Type typeOfT,
                                                 JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObj = json.getAsJsonObject();
      String className = jsonObj.get("classname").getAsString();
      Map<String, String> configs = context.deserialize(jsonObj.get("configs"),
                                                        new TypeToken<Map<String, String>>() {
                                                        }.getType());

      return new DefaultEventHandlerSpecification(className, configs);
    }
  }
}
