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

package co.cask.cdap.data2.metadata.system;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.data2.metadata.store.NoOpMetadataStore;
import co.cask.cdap.proto.id.DatasetId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test DatasetSystemMetadataWriter
 */
public class DatasetSystemMetadataWriterTest {
  @Test
  public void testFilesetSchema() throws Exception {
    DatasetProperties filesetAvroTableProps =
      FileSetProperties.builder()
        .setTableProperty(DatasetSystemMetadataWriter.FILESET_AVRO_SCHEMA_PROPERTY, "avro-table-schema")
        .build();
    assertDatasetSchema("avro-table-schema", filesetAvroTableProps);

    // When SCHEMA property is present, it should override
    filesetAvroTableProps =
      FileSetProperties.builder()
        .setTableProperty(DatasetSystemMetadataWriter.FILESET_AVRO_SCHEMA_PROPERTY, "avro-table-schema")
        .add(DatasetProperties.SCHEMA, "avro-schema")
        .build();
    assertDatasetSchema("avro-schema", filesetAvroTableProps);

    DatasetProperties filesetAvroOutputProps =
      FileSetProperties.builder()
        .setOutputProperty(DatasetSystemMetadataWriter.FILESET_AVRO_SCHEMA_OUTPUT_KEY, "avro-output-schema")
        .build();
    assertDatasetSchema("avro-output-schema", filesetAvroOutputProps);

    // When SCHEMA property is present, it should override
    filesetAvroOutputProps =
      FileSetProperties.builder()
        .setOutputProperty(DatasetSystemMetadataWriter.FILESET_AVRO_SCHEMA_OUTPUT_KEY, "avro-output-schema")
        .add(DatasetProperties.SCHEMA, "avro-schema")
        .build();
    assertDatasetSchema("avro-schema", filesetAvroOutputProps);

    DatasetProperties filesetParquetProps =
      FileSetProperties.builder()
        .setOutputProperty(DatasetSystemMetadataWriter.FILESET_PARQUET_SCHEMA_OUTPUT_KEY, "parquet-output-schema")
        .build();
    assertDatasetSchema("parquet-output-schema", filesetParquetProps);

    // When SCHEMA property is present, it should override
    filesetParquetProps =
      FileSetProperties.builder()
        .setOutputProperty(DatasetSystemMetadataWriter.FILESET_PARQUET_SCHEMA_OUTPUT_KEY, "parquet-output-schema")
        .add(DatasetProperties.SCHEMA, "parquet-schema")
        .build();
    assertDatasetSchema("parquet-schema", filesetParquetProps);
  }

  private void assertDatasetSchema(String expected, DatasetProperties properties) {
    DatasetSystemMetadataWriter metadataWriter =
      new DatasetSystemMetadataWriter(new NoOpMetadataStore(), new DatasetId("ns1", "avro1"),
                                      properties, null, null, null);
    Assert.assertEquals(expected, metadataWriter.getSchemaToAdd());
  }
}
