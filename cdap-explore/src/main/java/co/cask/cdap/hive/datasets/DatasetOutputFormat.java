/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.hive.datasets;

import co.cask.cdap.api.data.batch.RecordWritable;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.DatasetNotFoundException;
import co.cask.cdap.format.StructuredRecordStringConverter;
import com.google.gson.Gson;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.tephra.TransactionAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;

/**
 * Map reduce output format to write to datasets that implement {@link RecordWritable}.
 */
public class DatasetOutputFormat implements OutputFormat<Void, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetOutputFormat.class);

  @Override
  public RecordWriter<Void, Text> getRecordWriter(FileSystem ignored, final JobConf jobConf, String name,
                                                  Progressable progress) throws IOException {
    DatasetAccessor datasetAccessor = new DatasetAccessor(jobConf);
    try {
      datasetAccessor.initialize();
      return new DatasetRecordWriter(datasetAccessor);
    } catch (Exception e) {
      try {
        datasetAccessor.close();
      } catch (IOException e1) {
        LOG.warn("Exception closing dataset accessor after failure to return a DatasetRecordWriter.", e1);
      }
      throw new IOException("Could not get dataset.", e);
    }
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf jobConf) throws IOException {
    // This is called prior to returning a RecordWriter. We make sure here that the
    // dataset we want to write to is RecordWritable.
    try (DatasetAccessor datasetAccessor = new DatasetAccessor(jobConf)) {
      try {
        datasetAccessor.initialize();
      } catch (DatasetNotFoundException e) {
        throw new IOException(String.format("Dataset '%s' does not exist",
                                            datasetAccessor.getDatasetId()), e);
      } catch (DatasetManagementException | ClassNotFoundException e) {
        throw new IOException(String.format("Could not instantiate dataset '%s'", datasetAccessor.getDatasetId()), e);
      }
      if (!(datasetAccessor.getDataset() instanceof RecordWritable)) {
        throw new IOException(String.format("Dataset '%s' is not RecordWritable.",
                                            datasetAccessor.getDatasetId()));
      }
    }
  }

  private class DatasetRecordWriter implements RecordWriter<Void, Text> {
    private final DatasetAccessor datasetAccessor;
    private final RecordWritable recordWritable;
    private final Type recordType;
    private Schema recordSchema;

    DatasetRecordWriter(DatasetAccessor datasetAccessor) {
      this.datasetAccessor = datasetAccessor;
      this.recordWritable = datasetAccessor.getDataset();
      this.recordType = recordWritable.getRecordType();
      if (recordType == StructuredRecord.class) {
        try {
          DatasetSpecification datasetSpec = datasetAccessor.getDatasetSpec();
          String schemaStr = datasetSpec.getProperty(DatasetProperties.SCHEMA);
          // should never happen, as this should have been checked at table creation
          if (schemaStr == null) {
            throw new IllegalStateException(
              String.format("Dataset '%s' does not have the schema property.", datasetSpec.getName()));
          }
          recordSchema = Schema.parseJson(schemaStr);
        } catch (IOException | DatasetManagementException e) {
          try {
            recordWritable.close();
          } catch (IOException e1) {
            LOG.warn("Exception closing dataset {} after failing to look up its schema.",
                     datasetAccessor.getDatasetId(), e1);
          }
          throw new RuntimeException("Unable to look up schema for dataset.", e);
        }
      }
    }

    @Override
    public void write(Void key, Text value) throws IOException {
      if (value == null) {
        throw new IOException("Writable value is null.");
      }
      if (recordType == StructuredRecord.class) {
        recordWritable.write(StructuredRecordStringConverter.fromJsonString(value.toString(), recordSchema));
      } else {
        recordWritable.write(new Gson().fromJson(value.toString(), recordType));
      }
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      try {
        if (recordWritable instanceof TransactionAware) {
          try {
            // Commit changes made to the dataset being written
            // NOTE: because the transaction wrapping a Hive query is a long running one,
            // we don't track changes and don't check conflicts - we can just commit the changes.
            ((TransactionAware) recordWritable).commitTx();
          } catch (Exception e) {
            LOG.error("Could not commit changes for table {}", recordWritable);
            throw new IOException(e);
          }
        }
      } finally {
        recordWritable.close();
        datasetAccessor.close();
      }
    }
  }
}
