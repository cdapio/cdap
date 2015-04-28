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

package co.cask.cdap.templates.etl.transforms;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.TransformStage;
import co.cask.cdap.templates.etl.common.StructuredToAvroTransformer;
import org.apache.avro.generic.GenericRecord;

/**
 * Transform {@link StructuredRecord} to {@link GenericRecord}
 */
@Plugin(type = "transform")
@Name("StructuredRecordToGenericRecord")
@Description("Transforms a StructuredRecord into an Avro GenericRecord")
public class StructuredRecordToGenericRecordTransform extends TransformStage<StructuredRecord, GenericRecord> {
  private final StructuredToAvroTransformer transformer = new StructuredToAvroTransformer();

  @Override
  public void transform(StructuredRecord structuredRecord, Emitter<GenericRecord> emitter) throws Exception {
    emitter.emit(transformer.transform(structuredRecord));
  }
}
