/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.serialize;

import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import static com.continuuity.logging.serialize.Util.stringOrNull;

/**
 * Serializer for IThrowableProxy.
 */
public final class ThrowableProxySerializer {
  private ThrowableProxySerializer() {}

  public static GenericRecord encode(Schema schema, IThrowableProxy throwableProxy) {
    if (throwableProxy != null) {
      Schema tpSchema = schema.getTypes().get(1);
      GenericRecord datum = new GenericData.Record(tpSchema);
      datum.put("className", throwableProxy.getClassName());
      datum.put("message", throwableProxy.getMessage());
      datum.put("commonFramesCount", throwableProxy.getCommonFrames());
      datum.put("stackTraceElementProxyArray",
                StackTraceElementProxyArraySerializer.encode(tpSchema.getField("stackTraceElementProxyArray").schema(),
                                                             throwableProxy.getStackTraceElementProxyArray()));
      datum.put("cause", ThrowableProxySerializer.encode(tpSchema.getField("cause").schema(),
                                                         throwableProxy.getCause()));
      datum.put("suppressed", ThrowableProxyArraySerializer.encode(tpSchema.getField("suppressed").schema(),
                                                                   throwableProxy.getSuppressed()));
      return datum;
    }
    return null;
  }

  public static IThrowableProxy decode(GenericRecord datum) {
    if (datum != null) {
      String className = stringOrNull(datum.get("className"));
      String message = stringOrNull(datum.get("message"));
      int commonFramesCount = (Integer) datum.get("commonFramesCount");
      @SuppressWarnings("unchecked") StackTraceElementProxy[] steArray =
        StackTraceElementProxyArraySerializer.decode(
          (GenericArray<GenericRecord>) datum.get("stackTraceElementProxyArray"));
      IThrowableProxy cause = ThrowableProxySerializer.decode((GenericRecord) datum.get("cause"));
      @SuppressWarnings("unchecked") IThrowableProxy[] suppressed = ThrowableProxyArraySerializer.decode(
        (GenericArray<GenericRecord>) datum.get("suppressed"));
      return new ThrowableProxyImpl(cause, className, commonFramesCount, message, steArray, suppressed);
    }
    return null;
  }
}
