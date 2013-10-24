/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.serialize;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import static com.continuuity.logging.serialize.Util.stringOrNull;

/**
 * Serializer for StackTraceElement.
 */
public class StackTraceElementSerializer {
  private StackTraceElementSerializer() {}

  public static GenericRecord encode(Schema schema, StackTraceElement stackTraceElement) {
    if (stackTraceElement != null) {
      Schema steSchema = schema.getTypes().get(1);
      GenericRecord datum = new GenericData.Record(steSchema);
      datum.put("declaringClass", stackTraceElement.getClassName());
      datum.put("methodName", stackTraceElement.getMethodName());
      datum.put("fileName", stackTraceElement.getFileName());
      datum.put("lineNumber", stackTraceElement.getLineNumber());
      return datum;
    }
    return null;
  }

  public static StackTraceElement decode(GenericRecord datum) {
    if (datum != null) {
      String declaringClass =  stringOrNull(datum.get("declaringClass"));
      String methodName = stringOrNull(datum.get("methodName"));
      String fileName = stringOrNull(datum.get("fileName"));
      int lineNumber = (Integer) datum.get("lineNumber");
      return new StackTraceElement(declaringClass, methodName, fileName, lineNumber);
    }
    return null;
  }
}
