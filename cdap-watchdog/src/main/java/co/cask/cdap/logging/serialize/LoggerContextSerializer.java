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

import ch.qos.logback.classic.spi.LoggerContextVO;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.Map;

import static co.cask.cdap.logging.serialize.LogSerializerUtil.stringOrNull;

/**
 * Class used to serialize/de-serialize LoggerContextVO.
 */
final class LoggerContextSerializer {
  private LoggerContextSerializer() {}

  static GenericRecord encode(Schema schema, LoggerContextVO context) {
    if (context != null) {
      GenericRecord datum = new GenericData.Record(schema.getTypes().get(1));
      datum.put("birthTime", context.getBirthTime());
      datum.put("name", context.getName());
      datum.put("propertyMap", LogSerializerUtil.encodeMDC(context.getPropertyMap()));
      return datum;
    }
    return null;
  }

  static LoggerContextVO decode(GenericRecord datum) {
    if (datum != null) {
      long birthTime = (Long) datum.get("birthTime");
      String name = stringOrNull(datum.get("name"));
      //noinspection unchecked
      Map<String, String> propertyMap = LogSerializerUtil.decodeMDC((Map<?, ?>) datum.get("propertyMap"));
      return new LoggerContextVO(name, propertyMap, birthTime);
    }
    return null;
  }
}
