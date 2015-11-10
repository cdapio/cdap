/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utils class that contains StructuredRecord related transformations.
 */
public class StructuredRecordUtils {

  /**
   * Converts the field names in the input {@link StructuredRecord} to a desired case
   * @param input {@link StructuredRecord}
   * @param fieldCase {@link FieldCase}
   * @return {@link StructuredRecord} which contains field names confirming to the {@link FieldCase} passed in
   * @throws Exception if there is a conflict in the field names while converting the case
   */
  public static StructuredRecord convertCase(StructuredRecord input, FieldCase fieldCase) throws Exception {
    if (fieldCase.equals(FieldCase.NONE)) {
      return input;
    }

    Schema oldSchema = input.getSchema();
    Map<String, String> fieldNameMap = new HashMap<>();
    List<Schema.Field> newFields = new ArrayList<>();
    for (Schema.Field field : oldSchema.getFields()) {
      String newName = changeName(field.getName(), fieldCase);
      if (fieldNameMap.containsValue(newName)) {
        // field name used already. indication of field names conflict. can't do anything.
        throw new IllegalStateException(String.format(
          "Duplicate field/column name %s found when trying to confirm to the chosen case option %s. " +
            "Check Database Table schema.", field.getName(), fieldCase));
      }
      fieldNameMap.put(field.getName(), newName);
      newFields.add(Schema.Field.of(newName, field.getSchema()));
    }
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(Schema.recordOf("dbRecord", newFields));
    for (Map.Entry<String, String> nameMap : fieldNameMap.entrySet()) {
      recordBuilder.set(nameMap.getValue(), input.get(nameMap.getKey()));
    }
    return recordBuilder.build();
  }

  private StructuredRecordUtils() {
  }

  private static String changeName(String oldName, FieldCase fieldCase) {
    switch (fieldCase) {
      case LOWER:
        return oldName.toLowerCase();
      case UPPER:
        return oldName.toUpperCase();
      default:
        return oldName;
    }
  }
}
