package com.continuuity.internal.app.runtime.batch.dataset;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.batch.BatchWritable;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class DataSetOutputFormat extends OutputFormat {
  public static final String OUTPUT_DATASET_SPEC = "output.dataset.spec";

  public static void setOutput(Job job, DataSet dataSet) {
    job.setOutputFormatClass(DataSetOutputFormat.class);
    job.getConfiguration().set(OUTPUT_DATASET_SPEC, new Gson().toJson(dataSet.configure()));
  }

  @Override
  public RecordWriter getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    DataSetSpecification spec = getOutputDataSetSpec(conf);
    BatchWritable dataset =
      (BatchWritable) DataSetInputOutputFormatHelper.getDataSet(conf, spec);

    return new DataSetRecordWriter(dataset);
  }

  private DataSetSpecification getOutputDataSetSpec(Configuration conf) {
    return new Gson().fromJson(conf.get(OUTPUT_DATASET_SPEC), DataSetSpecification.class);
  }

  @Override
  public void checkOutputSpecs(final JobContext context) throws IOException, InterruptedException {
    // TODO: validate out types? Or this is ensured by configuring job in "internal" code (i.e. not in user code)
  }

  @Override
  public OutputCommitter getOutputCommitter(final TaskAttemptContext context) throws IOException, InterruptedException {
    return new DataSetOutputCommitter();
  }
}
