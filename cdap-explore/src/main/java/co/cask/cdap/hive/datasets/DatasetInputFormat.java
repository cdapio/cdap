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

import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Split;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Map reduce input format to read from datasets that implement RecordScannable.
 */
public class DatasetInputFormat implements InputFormat<Void, ObjectWritable> {
  private static final Gson GSON = new Gson();

  @Override
  public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
    try (DatasetAccessor datasetAccessor = new DatasetAccessor(jobConf)) {
      try {
        datasetAccessor.initialize();
      } catch (Exception e) {
        throw new IOException("Could not get dataset", e);
      }
      try (RecordScannable recordScannable = datasetAccessor.getDataset()) {
        Job job = new Job(jobConf);
        JobContext jobContext = ShimLoader.getHadoopShims().newJobContext(job);
        Path[] tablePaths = FileInputFormat.getInputPaths(jobContext);

        List<Split> dsSplits = recordScannable.getSplits();

        InputSplit[] inputSplits = new InputSplit[dsSplits.size()];
        for (int i = 0; i < dsSplits.size(); i++) {
          inputSplits[i] = new DatasetInputSplit(dsSplits.get(i), tablePaths[0]);
        }
        return inputSplits;
      }
    }
  }

  @Override
  public RecordReader<Void, ObjectWritable> getRecordReader(final InputSplit split, JobConf jobConf, Reporter reporter)
    throws IOException {

    if (!(split instanceof DatasetInputSplit)) {
      throw new IOException("Invalid type for InputSplit: " + split.getClass().getName());
    }
    final DatasetInputSplit datasetInputSplit = (DatasetInputSplit) split;

    return new DatasetRecordReader(jobConf, datasetInputSplit);
  }

  /**
   * This class duplicates all the functionality of
   * {@link co.cask.cdap.internal.app.runtime.batch.dataset.DataSetInputSplit}, but implements
   * {@link org.apache.hadoop.mapred.InputSplit} instead of {@link org.apache.hadoop.mapreduce.InputSplit}.
   */
  public static class DatasetInputSplit extends FileSplit {
    private Split dataSetSplit;

    // for Writable
    @SuppressWarnings("UnusedDeclaration")
    public DatasetInputSplit() {
    }

    public DatasetInputSplit(Split dataSetSplit, Path dummyPath) {
      super(dummyPath, 0, 0, (String[]) null);
      this.dataSetSplit = dataSetSplit;
    }

    public Split getDataSetSplit() {
      return dataSetSplit;
    }

    @Override
    public long getLength() {
      return dataSetSplit.getLength();
    }

    @Override
    public String[] getLocations() throws IOException {
      // TODO: Implement this functionality when DataSets has locality information.
      return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      Text.writeString(out, dataSetSplit.getClass().getName());
      String ser = GSON.toJson(dataSetSplit);
      Text.writeString(out, ser);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      try {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
          classLoader = getClass().getClassLoader();
        }
        Class<?> splitClass = classLoader.loadClass(Text.readString(in));
        if (!Split.class.isAssignableFrom(splitClass)) {
          throw new IllegalStateException("Cannot de-serialize Split class type! Got type " +
                                            splitClass.getCanonicalName());
        }
        //noinspection unchecked
        dataSetSplit = GSON.fromJson(Text.readString(in), (Class<? extends Split>) splitClass);
      } catch (ClassNotFoundException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private class DatasetRecordReader implements RecordReader<Void, ObjectWritable> {
    private final AtomicBoolean initialized;
    private final DatasetAccessor datasetAccessor;
    private final DatasetInputSplit datasetInputSplit;
    private RecordScannable recordScannable;
    private RecordScanner recordScanner;

    public DatasetRecordReader(Configuration conf, DatasetInputSplit datasetInputSplit) throws IOException {
      this.initialized = new AtomicBoolean(false);
      this.datasetAccessor = new DatasetAccessor(conf);
      this.datasetInputSplit = datasetInputSplit;
    }

    private void initialize() throws IOException {
      try {
        datasetAccessor.initialize();
        recordScannable = datasetAccessor.getDataset();
        recordScanner = recordScannable.createSplitRecordScanner(
          new Split() {
            @Override
            public long getLength() {
              return datasetInputSplit.getLength();
            }
          }
        );
        recordScanner.initialize(datasetInputSplit.getDataSetSplit());
        initialized.set(true);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while initializing reader", ie);
      } catch (Exception e) {
        throw new IOException("Unable to get dataset", e);
      }
    }

    @Override
    public boolean next(Void key, ObjectWritable value) throws IOException {
      if (!initialized.get()) {
        initialize();
      }

      try {
        boolean retVal = recordScanner.nextRecord();
        if (retVal) {
          value.set(recordScanner.getCurrentRecord());
        }
        return retVal;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }

    @Override
    public Void createKey() {
      return null;
    }

    @Override
    public ObjectWritable createValue() {
      return new ObjectWritable();
    }

    @Override
    public long getPos() throws IOException {
      // Not required.
      return 0;
    }

    @Override
    public void close() throws IOException {
      try {
        recordScanner.close();
      } finally {
        recordScannable.close();
        datasetAccessor.close();
      }
    }

    @Override
    public float getProgress() throws IOException {
      try {
        return recordScanner.getProgress();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }
  }
}
