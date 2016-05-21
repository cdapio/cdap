/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.metrics.Metrics;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 */
public class AppWithMapReduce extends AbstractApplication {
  @Override
  public void configure() {
    setName("AppWithMapReduce");
    setDescription("Application with MapReduce job");
    createDataset("beforeSubmit", KeyValueTable.class);
    createDataset("onFinish", KeyValueTable.class);
    createDataset("timeSeries", TimeseriesTable.class);
    createDataset("counters", Table.class);
    createDataset("countersFromContext", Table.class);
    addMapReduce(new ClassicWordCount());
    addMapReduce(new AggregateTimeseriesByTag());
  }

  /**
   *
   */
  public static final class ClassicWordCount extends AbstractMapReduce {
    public static final int MEMORY_MB = 1024;

    @UseDataSet("jobConfig")
    private KeyValueTable table;

    @Override
    protected void configure() {
      createDataset("jobConfig", KeyValueTable.class);
      setDriverResources(new Resources(MEMORY_MB));
    }

    @Override
    public void initialize() throws Exception {
      String inputPath = Bytes.toString(table.read(Bytes.toBytes("inputPath")));
      String outputPath = Bytes.toString(table.read(Bytes.toBytes("outputPath")));
      WordCount.configureJob((Job) getContext().getHadoopJob(), inputPath, outputPath);
    }
  }

  /**
   *
   */
  public static final class AggregateTimeseriesByTag extends AbstractMapReduce {
    @UseDataSet("beforeSubmit")
    private KeyValueTable beforeSubmitTable;
    @UseDataSet("onFinish")
    private KeyValueTable onFinishTable;
    @UseDataSet("timeSeries")
    private TimeseriesTable table;

    private Metrics metrics;

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      metrics.count("beforeSubmit", 1);
      Job hadoopJob = context.getHadoopJob();
      AggregateMetricsByTag.configureJob(hadoopJob);
      String metricName = context.getRuntimeArguments().get("metric");
      Long startTs = Long.valueOf(context.getRuntimeArguments().get("startTs"));
      Long stopTs = Long.valueOf(context.getRuntimeArguments().get("stopTs"));
      String tag = context.getRuntimeArguments().get("tag");
      context.addInput(Input.ofDataset("timeSeries", table.getInputSplits(2, Bytes.toBytes(metricName), startTs, stopTs,
                                                                          Bytes.toBytes(tag))));
      beforeSubmitTable.write(Bytes.toBytes("beforeSubmit"), Bytes.toBytes("beforeSubmit:done"));
      String frequentFlushing = context.getRuntimeArguments().get("frequentFlushing");
      if (frequentFlushing != null) {
        hadoopJob.getConfiguration().setInt("c.mapper.flush.freq", 1);
        hadoopJob.getConfiguration().setInt("c.reducer.flush.freq", 1);
      }
      metrics.count("beforeSubmit", 1);
      context.addOutput(Output.ofDataset("timeSeries"));
    }

    @Override
    public void destroy() {
      metrics.count("onFinish", 1);
      onFinishTable.write(Bytes.toBytes("onFinish"), Bytes.toBytes("onFinish:done"));
      metrics.count("onFinish", 1);
    }
  }
}
