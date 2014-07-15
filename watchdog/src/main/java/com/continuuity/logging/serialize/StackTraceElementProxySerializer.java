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
