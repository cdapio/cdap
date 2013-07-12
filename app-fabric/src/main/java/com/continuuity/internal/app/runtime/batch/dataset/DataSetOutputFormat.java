package com.continuuity.internal.app.runtime.batch.dataset;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.internal.app.runtime.batch.BasicMapReduceContext;
import com.continuuity.internal.app.runtime.batch.MapReduceContextProvider;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * An {@link OutputFormat} for writing into dataset.
 * @param <KEY> Type of key.
 * @param <VALUE> Type of value.
 */
public final class DataSetOutputFormat<KEY, VALUE> extends OutputFormat<KEY, VALUE> {
  public static final String OUTPUT_DATASET_SPEC = "output.dataset.spec";

  public static void setOutput(Job job, DataSet dataSet) {
    job.setOutputFormatClass(DataSetOutputFormat.class);
    job.getConfiguration().set(OUTPUT_DATASET_SPEC, new Gson().toJson(dataSet.configure()));
  }

  @Override
  public RecordWriter<KEY, VALUE> getRecordWriter(final TaskAttemptContext context)
    throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    MapReduceContextProvider contextProvider = new MapReduceContextProvider(context);
    BasicMapReduceContext mrContext = contextProvider.get();
    @SuppressWarnings("unchecked")
    BatchWritable<KEY, VALUE> dataset = (BatchWritable) mrContext.getDataSet(getOutputDataSetSpec(conf).getName());

    // the record writer now owns the context and will close it
    return new DataSetRecordWriter<KEY, VALUE>(dataset, mrContext);
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
