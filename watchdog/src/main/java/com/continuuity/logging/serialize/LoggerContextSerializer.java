/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.serialize;

import ch.qos.logback.classic.spi.LoggerContextVO;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.Map;

import static com.continuuity.logging.serialize.Util.stringOrNull;

/**
 * Class used to serialize/de-serialize LoggerContextVO.
 */
public final class LoggerContextSerializer {
  private LoggerContextSerializer() {}

  public static GenericRecord encode(Schema schema, LoggerContextVO context) {
    if (context != null) {
      GenericRecord datum = new GenericData.Record(schema.getTypes().get(1));
      datum.put("birthTime", context.getBirthTime());
      datum.put("name", context.getName());
      datum.put("propertyMap", LoggingEvent.encodeMdcMap(context.getPropertyMap()));
      return datum;
    }
    return null;
  }

  public static LoggerContextVO decode(GenericRecord datum) {
    if (datum != null) {
      long birthTime = (Long) datum.get("birthTime");
      String name = stringOrNull(datum.get("name"));
      //noinspection unchecked
      Map<String, String> propertyMap = LoggingEvent.decodeMdcMap((Map<?, ?>) datum.get("propertyMap"));
      return new LoggerContextVO(name, propertyMap, birthTime);
    }
    return null;
  }
}
