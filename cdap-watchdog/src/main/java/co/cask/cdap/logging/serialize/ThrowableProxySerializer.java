/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import static co.cask.cdap.logging.serialize.LogSerializerUtil.stringOrNull;

/**
 * Serializer for IThrowableProxy.
 */
final class ThrowableProxySerializer {
  private ThrowableProxySerializer() {}

  static GenericRecord encode(Schema schema, IThrowableProxy throwableProxy) {
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

  static IThrowableProxy decode(GenericRecord datum) {
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
