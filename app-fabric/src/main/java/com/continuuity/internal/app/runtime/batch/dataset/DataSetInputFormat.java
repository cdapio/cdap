package com.continuuity.internal.app.runtime.batch.dataset;

import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.app.metrics.MapReduceMetrics;
import com.continuuity.internal.app.runtime.batch.BasicMapReduceContext;
import com.continuuity.internal.app.runtime.batch.MapReduceContextConfig;
import com.continuuity.internal.app.runtime.batch.MapReduceContextProvider;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An {@link InputFormat} that reads from dataset.
 * @param <KEY> Type of key.
 * @param <VALUE> Type of value.
 */
public final class DataSetInputFormat<KEY, VALUE> extends InputFormat<KEY, VALUE> {
  private static final Logger LOG = LoggerFactory.getLogger(DataSetInputFormat.class);
  public static final String INPUT_DATASET_SPEC = "input.dataset.spec";

  public static void setInput(Job job, DataSetSpecification spec) {
    job.setInputFormatClass(DataSetInputFormat.class);
    job.getConfiguration().set(DataSetInputFormat.INPUT_DATASET_SPEC, new Gson().toJson(spec));
  }

  @Override
  public List<InputSplit> getSplits(final JobContext context) throws IOException, InterruptedException {
    MapReduceContextConfig mrContextConfig = new MapReduceContextConfig(context);
    List<Split> splits = mrContextConfig.getInputSelection();
    List<InputSplit> list = new ArrayList<InputSplit>();
    for (Split split : splits) {
      list.add(new DataSetInputSplit(split));
    }
    return list;
  }

  @Override
  public RecordReader<KEY, VALUE> createRecordReader(final InputSplit split,
                                                     final TaskAttemptContext context)
    throws IOException, InterruptedException {

    DataSetInputSplit inputSplit = (DataSetInputSplit) split;

    Configuration conf = context.getConfiguration();
    // we don't currently allow datasets as the format between map and reduce stages, otherwise we'll have to
    // pass in the stage here instead of hardcoding mapper.
    MapReduceContextProvider contextProvider = new MapReduceContextProvider(context, MapReduceMetrics.TaskType.Mapper);
    BasicMapReduceContext mrContext = contextProvider.get();
    mrContext.getMetricsCollectionService().startAndWait();
    @SuppressWarnings("unchecked")
    String dataSetName = getInputDataSetSpec(conf).getName();
    BatchReadable<KEY, VALUE> dataset = (BatchReadable) mrContext.getDataSet(dataSetName);
    SplitReader<KEY, VALUE> splitReader = dataset.createSplitReader(inputSplit.getSplit());

    // the record reader now owns the context and will close it
    return new DataSetRecordReader<KEY, VALUE>(splitReader, mrContext, dataSetName);
  }

  private DataSetSpecification getInputDataSetSpec(Configuration conf) {
    return new Gson().fromJson(conf.get(INPUT_DATASET_SPEC), DataSetSpecification.class);
  }
}
