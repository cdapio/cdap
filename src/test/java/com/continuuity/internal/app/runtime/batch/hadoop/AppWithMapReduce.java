package com.continuuity.internal.app.runtime.batch.hadoop;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.batch.hadoop.MapReduce;
import com.continuuity.api.batch.hadoop.MapReduceContext;
import com.continuuity.api.batch.hadoop.MapReduceSpecification;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.dataset.SimpleTimeseriesTable;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 */
public class AppWithMapReduce implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("AppWithMapReduce")
      .setDescription("Application with MapReduce job")
      .noStream()
      .withDataSets().add(new KeyValueTable("jobConfig")).add(new SimpleTimeseriesTable("timeSeries"))
      .noFlow()
      .noProcedure()
      .withBatch().add(new ClassicWordCount()).add(new AggregateTimeseriesByTag())
      .build();
  }

  public static final class ClassicWordCount implements MapReduce {
    @UseDataSet("jobConfig")
    private KeyValueTable table;

    @Override
    public MapReduceSpecification configure() {
      return MapReduceSpecification.Builder.with()
        .setName("ClassicWordCount")
        .setDescription("WordCount job from Hadoop examples")
        .build();
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      String inputPath = Bytes.toString(table.read(Bytes.toBytes("inputPath")));
      String outputPath = Bytes.toString(table.read(Bytes.toBytes("outputPath")));
      WordCount.configureJob((Job) context.getHadoopJobConf(), inputPath, outputPath);
    }

    @Override
    public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
      System.out.println("Action taken on MapReduce job " + (succeeded ? "" : "un") + "successful completion");
    }
  }

  public static final class AggregateTimeseriesByTag implements MapReduce {
    @UseDataSet("timeSeries")
    private SimpleTimeseriesTable table;

    @Override
    public MapReduceSpecification configure() {
      return MapReduceSpecification.Builder.with()
        .setName("AggMetricsByTag")
        .setDescription("Aggregates metrics values by tag")
          // no need to specify input dataset here as it is defined in beforeSubmit() below
//        .useInputDataSet("timeSeries")
        .useOutputDataSet("timeSeries")
        .build();
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      AggregateMetricsByTag.configureJob((Job) context.getHadoopJobConf());
      context.setInput(table, table.getInput(2, Bytes.toBytes("metric"), 1, 3, Bytes.toBytes("tag1")));
    }

    @Override
    public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
      System.out.println("Action taken on MapReduce job " + (succeeded ? "" : "un") + "successful completion");
    }
  }
}
