/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.tephra.Transaction;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * App to test proper transaction handling in the map reduce service.
 */
public class AppWithTxAware extends AbstractApplication {
  @Override
  public void configure() {
    setName("AppWithTxAware");
    setDescription("Application with MapReduce job that uses a TxAware dataset");
    createDataset("pedanticTxAware", PedanticTxAware.class);
    addMapReduce(new PedanticMapReduce());
  }

  /**
   * This m/r fails if its beforeSubmit() is not run in the same transaction as getSplits().
   */
  public static class PedanticMapReduce extends AbstractMapReduce {

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(DummyMapper.class);
      job.setNumReduceTasks(0);
      FileOutputFormat.setOutputPath(job, new Path(context.getRuntimeArguments().get("outputPath")));
      PedanticTxAware input = context.getDataset("pedanticTxAware", ImmutableMap.of("value", "1"));
      context.setInput("pedanticTxAware", input);
      input.rememberTx();
    }
  }

  public static class DummyMapper extends Mapper<Integer, Integer, Integer, Integer> {
    @Override
    protected void map(Integer key, Integer value, Context context) throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

  public static class PedanticTxAware extends AbstractDataset implements BatchReadable<Integer, Integer> {
    private Transaction tx = null;
    private Transaction remembered = null;

    public PedanticTxAware(DatasetSpecification spec, @EmbeddedDataset("t") Table embedded) {
      super(spec.getName(), embedded);
    }

    public void rememberTx() {
      this.remembered = this.tx;
    }

    @Override
    public void startTx(Transaction tx) {
      this.tx = tx;
      super.startTx(tx);
    }

    @Override
    public boolean commitTx() throws Exception {
      this.tx = null;
      return super.commitTx();
    }

    @Override
    public boolean rollbackTx() throws Exception {
      this.tx = null;
      return super.rollbackTx();
    }

    @Override
    public List<Split> getSplits() {
      Preconditions.checkNotNull(tx, "getSplits() called without a transaction");
      Preconditions.checkState(tx == remembered, "getSplits() called in different transaction");
      return Collections.singletonList((Split) new DummySplit());
    }

    public static class DummySplit extends Split {
      // this is actually never used except that the dataset wants to serialize it
    }

    @Override
    public SplitReader<Integer, Integer> createSplitReader(Split split) {
      return new SplitReader<Integer, Integer>() {
        boolean first = true;

        @Override
        public void initialize(Split split) throws InterruptedException {
          // do nothing
        }

        @Override
        public boolean nextKeyValue() throws InterruptedException {
          boolean result = first;
          first = false;
          return result;
        }

        @Override
        public Integer getCurrentKey() throws InterruptedException {
          return 1;
        }

        @Override
        public Integer getCurrentValue() throws InterruptedException {
          return 1;
        }

        @Override
        public void close() {
          // do nothing
        }
      };
    }
  }
}
