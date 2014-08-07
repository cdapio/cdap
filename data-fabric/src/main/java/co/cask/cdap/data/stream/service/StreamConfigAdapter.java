package co.cask.cdap.data.stream.service;

import co.cask.cdap.data2.transaction.stream.StreamConfig;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.concurrent.TimeUnit;

/**
 *  Serializer for {@link co.cask.cdap.data2.transaction.stream.StreamConfig} class
 */
class StreamConfigAdapter implements JsonSerializer<StreamConfig> {
  @Override
  public JsonElement serialize(StreamConfig src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();
    json.addProperty("partitionDuration", src.getPartitionDuration());
    json.addProperty("indexInterval", src.getIndexInterval());
    json.addProperty("ttl", TimeUnit.MILLISECONDS.toSeconds(src.getTTL()));
    return json;
  }
}
