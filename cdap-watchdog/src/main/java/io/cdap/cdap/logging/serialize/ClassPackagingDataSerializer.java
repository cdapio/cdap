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

import ch.qos.logback.classic.spi.ClassPackagingData;
import co.cask.cdap.logging.LoggingUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Serializer for ClassPackagingData.
 */
final class ClassPackagingDataSerializer {
  private ClassPackagingDataSerializer() {}

  static GenericRecord encode(Schema schema, ClassPackagingData classPackagingData) {
    if (classPackagingData != null) {
      GenericRecord datum = new GenericData.Record(schema.getTypes().get(1));
      datum.put("codeLocation", classPackagingData.getCodeLocation());
      datum.put("version", classPackagingData.getVersion());
      datum.put("exact", classPackagingData.isExact());
      return datum;
    }
    return null;
  }

  static ClassPackagingData decode(GenericRecord datum) {
    if (datum != null) {
      String codeLocation =  LoggingUtil.stringOrNull(datum.get("codeLocation"));
      String version = LoggingUtil.stringOrNull(datum.get("version"));
      boolean exact = (Boolean) datum.get("exact");
      return new ClassPackagingData(codeLocation, version, exact);
    }
    return null;
  }
}
