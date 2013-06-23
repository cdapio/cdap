/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.common.logging.logback.serialize;

import ch.qos.logback.classic.spi.IThrowableProxy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Serializer for ThrowableProxyArray.
 */
public final class ThrowableProxyArraySerializer {
  private ThrowableProxyArraySerializer() {}

  public static GenericArray<GenericRecord> encode(Schema schema, IThrowableProxy[] throwableProxies) {
    if (throwableProxies != null) {
      Schema tpArraySchema = schema.getTypes().get(1);
      GenericArray<GenericRecord> steArray = new GenericData.Array<GenericRecord>(throwableProxies.length,
                                                                                  tpArraySchema);
      for (IThrowableProxy tp : throwableProxies) {
        steArray.add(ThrowableProxySerializer.encode(tpArraySchema.getElementType(), tp));
      }
      return steArray;
    }
    return null;
  }

  public static IThrowableProxy[] decode(GenericArray<GenericRecord> datum) {
    if (datum != null) {
      IThrowableProxy[] throwableProxies = new IThrowableProxy[datum.size()];
      int i = 0;
      for (GenericRecord aDatum : datum) {
        throwableProxies[i++] = ThrowableProxySerializer.decode(aDatum);
      }
      return throwableProxies;
    }
    return null;
  }
}
