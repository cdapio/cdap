/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.metadata.elastic;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.metadata.MetadataEntity;
import org.junit.Assert;
import org.junit.Test;

public class MetadataDocumentTest {

  @Test
  public void testSchema() {
    Schema bytesArraySchema = Schema.arrayOf(Schema.of(Schema.Type.BYTES));
    Schema stringArraySchema = Schema.arrayOf(Schema.of(Schema.Type.STRING));
    Schema booleanBytesMapSchema = Schema.mapOf(Schema.of(Schema.Type.BOOLEAN), Schema.of(Schema.Type.BYTES));
    Schema nestedMapSchema = Schema.mapOf(bytesArraySchema, booleanBytesMapSchema);
    Schema record22Schema = Schema.recordOf("record22", Schema.Field.of("a", nestedMapSchema));
    Schema record22ArraySchema = Schema.arrayOf(record22Schema);
    Schema bytesDoubleMapSchema = Schema.mapOf(Schema.of(Schema.Type.BYTES), Schema.of(Schema.Type.DOUBLE));
    Schema record21Schema = Schema.recordOf("record21",
                                            Schema.Field.of("x", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("y", stringArraySchema),
                                            Schema.Field.of("z", bytesDoubleMapSchema));
    Schema record21to22MapSchema = Schema.mapOf(record21Schema, record22ArraySchema);
    Schema nullableIntSchema = Schema.nullableOf(Schema.of(Schema.Type.INT));
    Schema tripeUnionSchema = Schema.unionOf(Schema.of(Schema.Type.INT), Schema.of(Schema.Type.LONG),
                                             Schema.of(Schema.Type.NULL));
    Schema complexSchema = Schema.recordOf("record1",
                                           Schema.Field.of("map1", record21to22MapSchema),
                                           Schema.Field.of("i", nullableIntSchema),
                                           Schema.Field.of("j", tripeUnionSchema));
    Schema anotherComplexSchema = Schema.arrayOf(Schema.of(Schema.Type.STRING));
    Schema superComplexSchema = Schema.unionOf(complexSchema, anotherComplexSchema, Schema.of(Schema.Type.NULL));

    String[] results = MetadataDocument.Builder.parseSchema(MetadataEntity.ofDataset("ds"),
                                                            superComplexSchema.toString()).split(" ");
    String[] expected = {
      "record1", "record1:RECORD",
      "map1", "map1:MAP",
      "record21", "record21:RECORD",
      "x", "x:STRING",
      "y", "y:ARRAY",
      "z", "z:MAP",
      "record22", "record22:RECORD",
      "a", "a:MAP",
      "i", "i:INT",
      "j", "j:UNION",
    };
    Assert.assertArrayEquals(expected, results);
  }

  @Test
  public void testInvalidSchema() {
    Schema schema = Schema.recordOf("mystruct",
                                    Schema.Field.of("x", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("y", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("z", Schema.of(Schema.Type.DOUBLE)));
    String schemaString = schema.toString();
    MetadataEntity entity = MetadataEntity.ofDataset("ds");
    String[] results = MetadataDocument.Builder.parseSchema(entity, schemaString).split(" ");
    String[] expected = {"mystruct", "mystruct:RECORD", "x", "x:STRING", "y", "y:INT", "z", "z:DOUBLE"};
    Assert.assertArrayEquals(expected, results);

    String schemaWithInvalidTypes = schemaString.replace("string", "nosuchtype");
    Assert.assertEquals(schemaWithInvalidTypes, MetadataDocument.Builder.parseSchema(entity, schemaWithInvalidTypes));

    String truncatedSchema = schemaString.substring(10, schemaString.length() - 10);
    Assert.assertEquals(truncatedSchema, MetadataDocument.Builder.parseSchema(entity, truncatedSchema));
  }
}
