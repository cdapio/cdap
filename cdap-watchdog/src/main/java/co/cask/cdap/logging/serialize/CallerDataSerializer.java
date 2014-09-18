/*
 * Copyright © 2014 Cask Data, Inc.
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
