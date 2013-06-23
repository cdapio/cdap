package com.continuuity.internal.app.runtime.batch.hadoop;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.batch.MapReduce;
import com.continuuity.api.batch.MapReduceContext;
import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.dataset.SimpleTimeseriesTable;
import com.continuuity.api.data.dataset.table.Table;
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
      .withDataSets()
        .add(new KeyValueTable("jobConfig"))
        .add(new SimpleTimeseriesTable("timeSeries"))
        .add(new Table("counters"))
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
      WordCount.configureJob((Job) context.getHadoopJob(), inputPath, outputPath);
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
      AggregateMetricsByTag.configureJob((Job) context.getHadoopJob());
      String metricName = context.getRuntimeArguments().get("metric");
      Long startTs = Long.valueOf(context.getRuntimeArguments().get("startTs"));
      Long stopTs = Long.valueOf(context.getRuntimeArguments().get("stopTs"));
      String tag = context.getRuntimeArguments().get("tag");
      context.setInput(table, table.getInput(2, Bytes.toBytes(metricName), startTs, stopTs, Bytes.toBytes(tag)));
    }

    @Override
    public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
      System.out.println("Action taken on MapReduce job " + (succeeded ? "" : "un") + "successful completion");
    }
  }
}
