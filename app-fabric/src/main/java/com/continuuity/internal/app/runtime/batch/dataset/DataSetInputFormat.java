package com.continuuity.internal.app.runtime.batch.dataset;

import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.app.metrics.MapReduceMetrics;
import com.continuuity.internal.app.runtime.batch.BasicMapReduceContext;
import com.continuuity.internal.app.runtime.batch.MapReduceContextConfig;
import com.continuuity.internal.app.runtime.batch.MapReduceContextProvider;
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
  public static final String HCONF_ATTR_INPUT_DATASET = "input.dataset.name";

  public static void setInput(Job job, String inputDatasetName) {
    job.setInputFormatClass(DataSetInputFormat.class);
    job.getConfiguration().set(DataSetInputFormat.HCONF_ATTR_INPUT_DATASET, inputDatasetName);
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

  @SuppressWarnings("unchecked")
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
    String dataSetName = getInputName(conf);
    BatchReadable<KEY, VALUE> inputDataset = (BatchReadable<KEY, VALUE>) mrContext.getDataSet(dataSetName);
    SplitReader<KEY, VALUE> splitReader = inputDataset.createSplitReader(inputSplit.getSplit());

    // the record reader now owns the context and will close it
    return new DataSetRecordReader<KEY, VALUE>(splitReader, mrContext, dataSetName);
  }

  private String getInputName(Configuration conf) {
    return conf.get(HCONF_ATTR_INPUT_DATASET);
  }
}
