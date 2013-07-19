/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.serialize;

import ch.qos.logback.classic.spi.ClassPackagingData;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Serializer for StackTraceElementProxy.
 */
public final class StackTraceElementProxySerializer {
  private StackTraceElementProxySerializer() {}

  public static GenericRecord encode(Schema schema, StackTraceElementProxy stackTraceElementProxy) {
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("stackTraceElement", StackTraceElementSerializer.encode(schema.getField("stackTraceElement").schema(),
                                                        stackTraceElementProxy.getStackTraceElement()));
    datum.put("classPackagingData", ClassPackagingDataSerializer.encode(schema.getField("classPackagingData").schema(),
                                                         stackTraceElementProxy.getClassPackagingData()));
    return datum;
  }

  public static StackTraceElementProxy decode(GenericRecord datum) {
    StackTraceElement ste =
      StackTraceElementSerializer.decode((GenericRecord) datum.get("stackTraceElement"));
    ClassPackagingData cpd = ClassPackagingDataSerializer.decode((GenericRecord) datum.get("classPackagingData"));
    StackTraceElementProxy stackTraceElementProxy = new StackTraceElementProxy(ste);
    if (cpd != null) {
      stackTraceElementProxy.setClassPackagingData(cpd);
    }
    return stackTraceElementProxy;
  }
}
