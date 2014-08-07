package co.cask.cdap.data2.datafabric.dataset;

import co.cask.cdap.api.dataset.DatasetSpecification;
import com.continuuity.tephra.TxConstants;
import com.google.common.reflect.TypeToken;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Serializer for {@link co.cask.cdap.api.dataset.DatasetSpecification} class
 */
public class DatsetSpecificationAdapter implements JsonSerializer<DatasetSpecification> {
  @Override
  public JsonElement serialize(DatasetSpecification src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("name", src.getName());
    jsonObject.addProperty("type", src.getType());
    JsonObject properties = new JsonObject();
    for (Map.Entry<String, String> property : src.getProperties().entrySet()) {
      if (property.getKey().equals(TxConstants.PROPERTY_TTL)) {
        long value = TimeUnit.MILLISECONDS.toSeconds(Long.parseLong(property.getValue()));
        properties.addProperty(property.getKey(), value);
      } else {
        properties.addProperty(property.getKey(), property.getValue());
      }
    }
    jsonObject.add("properties", properties);
    Type specsType = new TypeToken<Map<String, DatasetSpecification>>() { }.getType();
    jsonObject.add("datasetSpecs", context.serialize(src.getSpecifications(), specsType));
    return jsonObject;
  }
}
