/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.logging.serialize;

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
