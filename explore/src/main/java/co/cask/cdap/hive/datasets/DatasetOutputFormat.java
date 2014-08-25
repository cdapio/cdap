/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.hive.datasets;

import co.cask.cdap.api.data.batch.RecordWritable;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.hive.context.ConfigurationUtil;
import co.cask.cdap.hive.context.TxnCodec;
import com.continuuity.tephra.Transaction;
import com.continuuity.tephra.TransactionAware;
import com.continuuity.tephra.TransactionSystemClient;
import com.continuuity.tephra.TxConstants;
import com.continuuity.tephra.distributed.PooledClientProvider;
import com.continuuity.tephra.distributed.ThreadLocalClientProvider;
import com.continuuity.tephra.distributed.ThriftClientProvider;
import com.continuuity.tephra.distributed.TransactionServiceClient;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class DatasetOutputFormat implements OutputFormat<Void, ObjectWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetOutputFormat.class);

  public static final Map<String, List<byte[]>> QUERY_TO_CHANGES = Maps.newHashMap();

  // Should store a map of JobConf to transactionClient
  private static TransactionSystemClient txClient = null;

  @Override
  public RecordWriter<Void, ObjectWritable> getRecordWriter(FileSystem ignored, final JobConf jobConf, String name,
                                                            Progressable progress) throws IOException {
    // TODO the same transaction is started everytime we call this, is that a problem?
    final RecordWritable recordWritable = DatasetAccessor.getRecordWritable(jobConf);

    return new RecordWriter<Void, ObjectWritable>() {
      @Override
      public void write(Void key, ObjectWritable value) throws IOException {
        recordWritable.write(value.get());
      }

      @Override
      public void close(Reporter reporter) throws IOException {
        // TODO save the tx changes here. How to get them from the MR job to the CLI service though?...
        // If we add them to the jobconf, is it gonna be send back? Don't think so
        // TODO check jobconf contains this query ID
        if (recordWritable instanceof TransactionAware) {
          TransactionAware txAware = (TransactionAware) recordWritable;
          Transaction tx = ConfigurationUtil.get(jobConf, Constants.Explore.TX_QUERY_KEY,
                                                 TxnCodec.INSTANCE);
          Collection<byte[]> changes = txAware.getTxChanges();
          try {
            if (!getTransactionClient(jobConf).canCommit(tx, changes)) {
              txAware.rollbackTx();
              // TODO do we really want to abord here?
              getTransactionClient(jobConf).abort(tx);
            } else {
              txAware.commitTx();
            }
          } catch (Throwable t) {
            throw new IOException("Problem when committing changes for dataset " + recordWritable, t);
          }

//          String queryId = jobConf.get(Constants.Explore.QUERY_ID);
//          List<byte[]> changes = QUERY_TO_CHANGES.get(queryId);
//          if (changes == null) {
//            changes = Lists.newArrayList();
//          }
//          changes.addAll(((TransactionAware) recordWritable).getTxChanges());
//
//          QUERY_TO_CHANGES.put(queryId, changes);
//
//          try {
//            // Commit changes at the table level
//            // TODO is it relevent to do that here?
//            ((TransactionAware) recordWritable).commitTx();
//          } catch (Exception e) {
//            LOG.error("Could not commit changes for table {}", recordWritable);
//            throw new IOException(e);
//          }
        }
      }
    };
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    // This is called prior to returning a RecordWriter. We make sure here that the
    // dataset we want to write to is RecordWritable. This call will throw an exception
    // if it is not the case
    // TODO is this going to restart the transaction. We don't want that, just do the checkings
    DatasetAccessor.getRecordWritable(job);
  }

  private TransactionSystemClient getTransactionClient(JobConf jobConf) {
    if (txClient != null) {
      return txClient;
    }

    DiscoveryServiceClient discoveryServiceClient = new InMemoryDiscoveryService();

    // configure the client provider
    ThriftClientProvider thriftClientProvider;
    String provider = jobConf.get(TxConstants.Service.CFG_DATA_TX_CLIENT_PROVIDER,
                                  TxConstants.Service.DEFAULT_DATA_TX_CLIENT_PROVIDER);
    if ("pool".equals(provider)) {
      thriftClientProvider = new PooledClientProvider(jobConf, discoveryServiceClient);
    } else if ("thread-local".equals(provider)) {
      thriftClientProvider = new ThreadLocalClientProvider(jobConf, discoveryServiceClient);
    } else {
      String message = "Unknown Transaction Service Client Provider '" + provider + "'.";
      throw new IllegalArgumentException(message);
    }
    txClient = new TransactionServiceClient(jobConf, thriftClientProvider);
    return txClient;
  }
}
