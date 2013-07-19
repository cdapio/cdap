/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.serialize;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Serializer for CallerData.
 */
public final class CallerDataSerializer {
  private CallerDataSerializer() {}

  public static GenericArray<GenericRecord> encode(Schema schema, StackTraceElement[] stackTraceElements) {
    if (stackTraceElements != null) {
      Schema steArraySchema = schema.getTypes().get(1);
      GenericArray<GenericRecord> steArray = new GenericData.Array<GenericRecord>(stackTraceElements.length,
                                                                                  steArraySchema);
      for (StackTraceElement stackTraceElement : stackTraceElements) {
        steArray.add(StackTraceElementSerializer.encode(steArraySchema.getElementType(), stackTraceElement));
      }
      return steArray;
    }
    return null;
  }

  public static StackTraceElement[] decode(GenericArray<GenericRecord> datum) {
    if (datum != null) {
      StackTraceElement[] stackTraceElements = new StackTraceElement[datum.size()];
      int i = 0;
      for (GenericRecord aDatum : datum) {
        stackTraceElements[i++] = StackTraceElementSerializer.decode(aDatum);
      }
      return stackTraceElements;
    }
    return null;
  }
}
