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

package co.cask.cdap.internal.io;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.io.IOException;

/**
 * Decodes an object from a {@link Row} object fetched from a {@link Table} into a {@link StructuredRecord.Builder}
 * that is ready to be built. Used for exploration of Tables using only a schema without requiring a type token.
 */
public class ReflectionRowRecordReader extends ReflectionRowReader<StructuredRecord.Builder> {

  public ReflectionRowRecordReader(Schema schema) {
    super(schema, TypeToken.of(StructuredRecord.Builder.class));
    Preconditions.checkArgument(schema.getType() == Schema.Type.RECORD, "Target schema must be a record.");
  }

  @Override
  public StructuredRecord.Builder read(Row row, Schema sourceSchema) throws IOException {
    Preconditions.checkArgument(sourceSchema.getType() == Schema.Type.RECORD, "Source schema must be a record.");
    initializeRead(sourceSchema);
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    try {
      for (Schema.Field sourceField : sourceSchema.getFields()) {
        String sourceFieldName = sourceField.getName();
        Schema.Field targetField = schema.getField(sourceFieldName);
        if (targetField == null) {
          advanceField();
          continue;
        }
        builder.set(sourceFieldName, read(row, sourceField.getSchema(), targetField.getSchema(), type));
      }
      return builder;
    } catch (Exception e) {
      throw propagate(e);
    }
  }
}
