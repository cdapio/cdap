/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.indexer;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.data2.metadata.dataset.MetadataEntry;
import co.cask.cdap.proto.id.DatasetId;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

/**
 * Test {@link SchemaIndexer}.
 */
public class SchemaIndexerTest {

  @Test
  public void testSimpleSchema() throws Exception {
    Schema simpleSchema = Schema.of(Schema.Type.INT);
    Set<String> expected = Collections.emptySet();
    SchemaIndexer indexer = new SchemaIndexer();
    DatasetId datasetInstance = new DatasetId("ns1", "ds1");
    Set<String> actual = indexer.getIndexes(new MetadataEntry(datasetInstance, "schema", simpleSchema.toString()));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSimpleRecord() throws Exception {
    Schema simpleSchema = Schema.recordOf("record1",
                                          // String x
                                          Schema.Field.of("x", Schema.of(Schema.Type.STRING)),
                                          // String[] y
                                          Schema.Field.of("y", Schema.arrayOf(Schema.of(Schema.Type.STRING))),
                                          // Map<byte[],double> z
                                          Schema.Field.of("z", Schema.mapOf(Schema.of(Schema.Type.BYTES),
                                                                            Schema.of(Schema.Type.DOUBLE))));
    Set<String> expected = ImmutableSet.of("record1", "record1:RECORD", "x", "x:STRING", "y", "y:ARRAY", "z", "z:MAP");
    SchemaIndexer indexer = new SchemaIndexer();
    DatasetId datasetInstance = new DatasetId("ns1", "ds1");
    Set<String> actual = indexer.getIndexes(new MetadataEntry(datasetInstance, "schema", simpleSchema.toString()));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testComplexRecord() throws Exception {
    Schema complexSchema = Schema.recordOf(
      "record1",
      Schema.Field.of(
        "map1",
        Schema.mapOf(
          Schema.recordOf("record21",
                          // String x
                          Schema.Field.of("x", Schema.of(Schema.Type.STRING)),
                          // String[] y
                          Schema.Field.of("y", Schema.arrayOf(Schema.of(Schema.Type.STRING))),
                          // Map<byte[],double> z
                          Schema.Field.of("z", Schema.mapOf(Schema.of(Schema.Type.BYTES),
                                                            Schema.of(Schema.Type.DOUBLE)))),
          Schema.arrayOf(Schema.recordOf(
            "record22",
            Schema.Field.of("a",
                            // Map<array<byte[]>, Map<boolean,byte[]> a
                            Schema.mapOf(Schema.arrayOf(Schema.of(Schema.Type.BYTES)),
                                         Schema.mapOf(Schema.of(Schema.Type.BOOLEAN),
                                                      Schema.of(Schema.Type.BYTES)))
            )))
        )),
      Schema.Field.of("i", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("j", Schema.unionOf(Schema.of(Schema.Type.INT), Schema.of(Schema.Type.LONG),
                                          Schema.of(Schema.Type.NULL))));
    Schema anotherComplexSchema = Schema.arrayOf(Schema.of(Schema.Type.STRING));

    Schema superComplexSchema = Schema.unionOf(complexSchema, anotherComplexSchema, Schema.of(Schema.Type.NULL));

    Set<String> expected = ImmutableSet.of("map1", "map1:MAP", "record21", "record21:RECORD", "x", "x:STRING",
                                           "y", "y:ARRAY", "z", "z:MAP", "record22", "record22:RECORD", "a", "a:MAP",
                                           "i", "i:INT", "j", "j:UNION", "record1", "record1:RECORD");
    SchemaIndexer indexer = new SchemaIndexer();
    DatasetId datasetInstance = new DatasetId("ns1", "ds1");
    Set<String> actual = indexer.getIndexes(new MetadataEntry(datasetInstance, "schema",
                                                              superComplexSchema.toString()));
    Assert.assertEquals(expected, actual);
  }
}
