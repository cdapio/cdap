package com.continuuity.common.logging.logback.serialize;

import ch.qos.logback.classic.spi.ClassPackagingData;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Serializer for StackTraceElementProxy.
 */
public class StackTraceElementProxySerializer {
  public static GenericRecord encode(Schema schema, StackTraceElementProxy stackTraceElementProxy) {
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("ste", StackTraceElementSerializer.encode(schema.getField("ste").schema(),
                                                        stackTraceElementProxy.getStackTraceElement()));
    datum.put("cpd", ClassPackagingDataSerializer.encode(schema.getField("cpd").schema(),
                                                         stackTraceElementProxy.getClassPackagingData()));
    return datum;
  }

  public static StackTraceElementProxy decode(GenericRecord datum) {
    StackTraceElement ste =
      StackTraceElementSerializer.decode((GenericRecord) datum.get("ste"));
    ClassPackagingData cpd = ClassPackagingDataSerializer.decode((GenericRecord) datum.get("cpd"));
    StackTraceElementProxy stackTraceElementProxy = new StackTraceElementProxy(ste);
    if (cpd != null) {
      stackTraceElementProxy.setClassPackagingData(cpd);
    }
    return stackTraceElementProxy;
  }
}
