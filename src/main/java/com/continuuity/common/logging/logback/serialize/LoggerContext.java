package com.continuuity.common.logging.logback.serialize;

import ch.qos.logback.classic.spi.LoggerContextVO;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.Map;

/**
 * Class used to serialize/de-serialize LoggerContextVO.
 */
public class LoggerContext {
  public static GenericRecord encode(Schema schema, LoggerContextVO context) {
    if (context != null) {
      GenericRecord datum = new GenericData.Record(schema.getTypes().get(1));
      datum.put("birthTime", context.getBirthTime());
      datum.put("name", context.getName());
      datum.put("propertyMap", context.getPropertyMap());
      return datum;
    }
    return null;
  }

  public static LoggerContextVO decode(GenericRecord datum) {
    if (datum != null) {
      long birthTime = (Long) datum.get("birthTime");
      String name = datum.get("name").toString();
      //noinspection unchecked
      Map<String, String> propertyMap = (Map<String, String>) datum.get("propertyMap");
      return new LoggerContextVO(name, propertyMap, birthTime);
    }
    return null;
  }
}
