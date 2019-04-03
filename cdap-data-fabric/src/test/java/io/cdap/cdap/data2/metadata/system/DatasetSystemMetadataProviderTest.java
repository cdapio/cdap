/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

package io.cdap.cdap.data2.metadata.system;

import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.proto.id.DatasetId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test DatasetSystemMetadataProvider
 */
public class DatasetSystemMetadataProviderTest {

  @Test
  public void testFilesetSchema() {
    DatasetProperties filesetAvroTableProps =
      FileSetProperties.builder()
        .setTableProperty(DatasetSystemMetadataProvider.FILESET_AVRO_SCHEMA_PROPERTY, "avro-table-schema")
        .build();
    assertDatasetSchema("avro-table-schema", filesetAvroTableProps);

    // When SCHEMA property is present, it should override
    filesetAvroTableProps =
      FileSetProperties.builder()
        .setTableProperty(DatasetSystemMetadataProvider.FILESET_AVRO_SCHEMA_PROPERTY, "avro-table-schema")
        .add(DatasetProperties.SCHEMA, "avro-schema")
        .build();
    assertDatasetSchema("avro-schema", filesetAvroTableProps);

    DatasetProperties filesetAvroOutputProps =
      FileSetProperties.builder()
        .setOutputProperty(DatasetSystemMetadataProvider.FILESET_AVRO_SCHEMA_OUTPUT_KEY, "avro-output-schema")
        .build();
    assertDatasetSchema("avro-output-schema", filesetAvroOutputProps);

    // When SCHEMA property is present, it should override
    filesetAvroOutputProps =
      FileSetProperties.builder()
        .setOutputProperty(DatasetSystemMetadataProvider.FILESET_AVRO_SCHEMA_OUTPUT_KEY, "avro-output-schema")
        .add(DatasetProperties.SCHEMA, "avro-schema")
        .build();
    assertDatasetSchema("avro-schema", filesetAvroOutputProps);

    DatasetProperties filesetParquetProps =
      FileSetProperties.builder()
        .setOutputProperty(DatasetSystemMetadataProvider.FILESET_PARQUET_SCHEMA_OUTPUT_KEY, "parquet-output-schema")
        .build();
    assertDatasetSchema("parquet-output-schema", filesetParquetProps);

    // When SCHEMA property is present, it should override
    filesetParquetProps =
      FileSetProperties.builder()
        .setOutputProperty(DatasetSystemMetadataProvider.FILESET_PARQUET_SCHEMA_OUTPUT_KEY, "parquet-output-schema")
        .add(DatasetProperties.SCHEMA, "parquet-schema")
        .build();
    assertDatasetSchema("parquet-schema", filesetParquetProps);
  }

  private void assertDatasetSchema(String expected, DatasetProperties properties) {
    DatasetSystemMetadataProvider metadataWriter =
      new DatasetSystemMetadataProvider(new DatasetId("ns1", "avro1"), properties, null, null, null);
    Assert.assertEquals(expected, metadataWriter.getSchemaToAdd());
  }
}
