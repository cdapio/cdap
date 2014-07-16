/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.continuuity.logging.serialize;

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
