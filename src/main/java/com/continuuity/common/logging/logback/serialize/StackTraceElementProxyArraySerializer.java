/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.common.logging.logback.serialize;

import ch.qos.logback.classic.spi.StackTraceElementProxy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Serializer for an array of StackTraceElementProxies.
 */
public final class StackTraceElementProxyArraySerializer {
  private StackTraceElementProxyArraySerializer() {}

  public static GenericArray<GenericRecord> encode(Schema schema, StackTraceElementProxy[] stackTraceElementProxies) {
    if (stackTraceElementProxies != null) {
      Schema steArraySchema = schema.getTypes().get(1);
      GenericArray<GenericRecord> steArray = new GenericData.Array<GenericRecord>(stackTraceElementProxies.length,
                                                                                  steArraySchema);
      for (StackTraceElementProxy ste : stackTraceElementProxies) {
        steArray.add(StackTraceElementProxySerializer.encode(steArraySchema.getElementType(), ste));
      }
      return steArray;
    }
    return null;
  }

  public static StackTraceElementProxy[] decode(GenericArray<GenericRecord> datum) {
    if (datum != null) {
      StackTraceElementProxy[] stackTraceElementProxies = new StackTraceElementProxy[datum.size()];
      int i = 0;
      for (GenericRecord aDatum : datum) {
        stackTraceElementProxies[i++] = StackTraceElementProxySerializer.decode(aDatum);
      }
      return stackTraceElementProxies;
    }
    return null;
  }
}
