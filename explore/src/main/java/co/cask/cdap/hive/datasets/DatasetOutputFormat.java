/*
 * Copyright 2014 Cask Data, Inc.
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
import com.continuuity.tephra.TransactionAware;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Map reduce output format to write to datasets that implement {@link RecordWritable}.
 */
public class DatasetOutputFormat implements OutputFormat<Void, ObjectWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetOutputFormat.class);

  @Override
  public RecordWriter<Void, ObjectWritable> getRecordWriter(FileSystem ignored, final JobConf jobConf, String name,
                                                            Progressable progress) throws IOException {
    final RecordWritable recordWritable = DatasetAccessor.getRecordWritable(jobConf);
    final Type recordType = recordWritable.getRecordType();

    return new RecordWriter<Void, ObjectWritable>() {
      @Override
      public void write(Void key, ObjectWritable value) throws IOException {
        // Here we try to build a record object using the array of objects present in the writable.
        // We assume that a constructor with as many params as the number of attributes in the record
        // type exists for that type.

        if (value == null) {
          throw new IOException("Writable value is null.");
        }

        Object [] objects = ((List<Object>) value.get()).toArray();
        Class<?> [] classes = new Class[objects.length];
        for (int i = 0; i < objects.length; i++) {
          classes[i] = objects[i].getClass();
        }
        if (!(recordType instanceof Class)) {
          throw new RuntimeException(recordType + " should be a class");
        }
        Class<?> recordClass = (Class<?>) recordType;
        try {
          // Here we assume that the record type has a constructor that accepts the same types of parameters
          // as the list of objects contained in the writable.
          // TODO modify this, as the types can vary significantly. For example, in this query:
          // insert into table T1 select word from T2 - let's assume T1's schema is only one String column
          // Depending on the SerDe of the T2 table, the type that we get here can be String/LazyString/Text
          // So T1 record type would have to have at least 3 constructors accepting those types to handle
          // all possible cases.
          Constructor<?> constructor = recordClass.getConstructor(classes);
          recordWritable.write(constructor.newInstance(objects));
        } catch (NoSuchMethodException e) {
          throw new IOException(String.format("Could not find appropriate constructor to " +
                                                "build record object for type %s", recordType),
                                e);
        } catch (Throwable e) {
          throw new IOException(String.format("Could not build record object for type %s", recordType), e);
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
        }
      }
    };
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    // This is called prior to returning a RecordWriter. We make sure here that the
    // dataset we want to write to is RecordWritable.
    DatasetAccessor.checkRecordWritable(job);
  }
}
