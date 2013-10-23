package com.continuuity.internal.app.runtime.batch.dataset;

import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.app.metrics.MapReduceMetrics;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * An {@link OutputFormat} for writing into dataset.
 * @param <KEY> Type of key.
 * @param <VALUE> Type of value.
 */
public final class DataSetOutputFormat<KEY, VALUE> extends OutputFormat<KEY, VALUE> {
  private static final Logger LOG = LoggerFactory.getLogger(DataSetOutputFormat.class);
  public static final String OUTPUT_DATASET_SPEC = "output.dataset.spec";

  public static void setOutput(Job job, DataSetSpecification spec) {
    job.setOutputFormatClass(DataSetOutputFormat.class);
    job.getConfiguration().set(OUTPUT_DATASET_SPEC, new Gson().toJson(spec));
  }

  @Override
  public RecordWriter<KEY, VALUE> getRecordWriter(final TaskAttemptContext context)
    throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    // we don't currently allow datasets as the format between map and reduce stages, otherwise we'll have to
    // pass in the stage here instead of hardcoding reducer.
    MapReduceContextProvider contextProvider = new MapReduceContextProvider(context, MapReduceMetrics.TaskType.Reducer);
    BasicMapReduceContext mrContext = contextProvider.get();
    mrContext.getMetricsCollectionService().startAndWait();
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
