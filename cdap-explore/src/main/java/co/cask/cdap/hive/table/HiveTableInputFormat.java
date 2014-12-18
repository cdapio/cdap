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

package co.cask.cdap.hive.table;

import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.dataset.table.TableSplit;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.hive.context.ConfigurationUtil;
import co.cask.cdap.hive.context.ContextManager;
import co.cask.cdap.hive.context.TxnCodec;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Table input format for use in hive queries and only hive queries. Will not work outside of hive.
 */
public class HiveTableInputFormat implements InputFormat<Void, ObjectWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(HiveTableInputFormat.class);

  @Override
  public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
    String tableName = conf.get(Constants.Explore.TABLE_DATASET_NAME);
    Table table = getTable(conf, tableName);

    try {
      Job job = new Job(conf);
      JobContext jobContext = ShimLoader.getHadoopShims().newJobContext(job);
      Path[] tablePaths = FileInputFormat.getInputPaths(jobContext);

      List<Split> dsSplits = table.getSplits();

      InputSplit[] inputSplits = new InputSplit[dsSplits.size()];
      for (int i = 0; i < dsSplits.size(); i++) {
        TableSplit tableSplit = (TableSplit) dsSplits.get(i);
        inputSplits[i] = new TableInputSplit(tableName, tableSplit.getStart(), tableSplit.getStop(), tablePaths[0]);
      }
      return inputSplits;
    } finally {
      table.close();
    }
  }

  private Table getTable(Configuration conf, String tableName) throws IOException {
    ContextManager.Context context = ContextManager.getContext(conf);
    try {
      Table table = context.getDatasetFramework().getDataset(tableName, Collections.<String, String>emptyMap(), null);
      if (table instanceof TransactionAware) {
        Transaction tx = ConfigurationUtil.get(conf, Constants.Explore.TX_QUERY_KEY, TxnCodec.INSTANCE);
        ((TransactionAware) table).startTx(tx);
      }
      return table;
    } catch (DatasetManagementException e) {
      LOG.error("Unable to get TableType to instantiate table {}.", tableName, e);
      throw new IOException(e);
    }
  }

  @Override
  public RecordReader<Void, ObjectWritable> getRecordReader(InputSplit split, JobConf conf, Reporter reporter)
    throws IOException {
    TableInputSplit tableInputSplit = (TableInputSplit) split;
    return new TableRecordReader(tableInputSplit, getTable(conf, tableInputSplit.getTableName()));
  }
}
