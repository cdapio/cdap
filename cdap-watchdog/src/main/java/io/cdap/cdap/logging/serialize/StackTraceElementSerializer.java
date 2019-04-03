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

import co.cask.cdap.logging.LoggingUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Serializer for StackTraceElement.
 */
class StackTraceElementSerializer {
  private StackTraceElementSerializer() {}

  static GenericRecord encode(Schema schema, StackTraceElement stackTraceElement) {
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

  static StackTraceElement decode(GenericRecord datum) {
    if (datum != null) {
      String declaringClass = LoggingUtil.stringOrNull(datum.get("declaringClass"));
      String methodName = LoggingUtil.stringOrNull(datum.get("methodName"));
      String fileName = LoggingUtil.stringOrNull(datum.get("fileName"));
      int lineNumber = (Integer) datum.get("lineNumber");
      return new StackTraceElement(declaringClass, methodName, fileName, lineNumber);
    }
    return null;
  }
}
