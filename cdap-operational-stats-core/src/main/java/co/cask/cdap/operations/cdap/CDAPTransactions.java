/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.operations.cdap;

import co.cask.cdap.operations.OperationalStats;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.ChangeId;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.persist.TransactionSnapshot;
import org.apache.tephra.snapshot.SnapshotCodecProvider;

import java.io.InputStream;
import java.util.Set;

/**
 * {@link OperationalStats} for reporting CDAP transaction statistics.
 */
public class CDAPTransactions extends AbstractCDAPStats implements CDAPTransactionsMXBean {

  private TransactionSystemClient txClient;
  private SnapshotCodecProvider codecProvider;
  private int numInvalidTx;
  private long snapshotTime;
  private long readPointer;
  private long writePointer;
  private long visibilityUpperBound;
  private int numInProgressTx;
  private int numCommittingChangeSets;
  private int numCommittedChangeSets;

  @Override
  public void initialize(Injector injector) {
    txClient = injector.getInstance(TransactionSystemClient.class);
    codecProvider = new SnapshotCodecProvider(injector.getInstance(Configuration.class));
  }

  @Override
  public String getStatType() {
    return "transactions";
  }

  @Override
  public long getSnapshotTime() {
    return snapshotTime;
  }

  @Override
  public long getReadPointer() {
    return readPointer;
  }

  @Override
  public long getWritePointer() {
    return writePointer;
  }

  @Override
  public int getNumInProgressTransactions() {
    return numInProgressTx;
  }

  @Override
  public int getNumInvalidTransactions() {
    return numInvalidTx;
  }

  @Override
  public int getNumCommittingChangeSets() {
    return numCommittingChangeSets;
  }

  @Override
  public int getNumCommittedChangeSets() {
    return numCommittedChangeSets;
  }

  @Override
  public long getVisibilityUpperBound() {
    return visibilityUpperBound;
  }

  @Override
  public void collect() throws Exception {
    numInvalidTx = txClient.getInvalidSize();
    TransactionSnapshot txSnapshot;
    try (InputStream inputStream = txClient.getSnapshotInputStream()) {
      txSnapshot = codecProvider.decode(inputStream);
    }
    snapshotTime = txSnapshot.getTimestamp();
    readPointer = txSnapshot.getReadPointer();
    writePointer = txSnapshot.getWritePointer();
    numInProgressTx = txSnapshot.getInProgress().size();
    numCommittingChangeSets = 0;
    for (Set<ChangeId> changeIds : txSnapshot.getCommittingChangeSets().values()) {
      numCommittingChangeSets += changeIds.size();
    }
    numCommittedChangeSets = 0;
    for (Set<ChangeId> changeIds : txSnapshot.getCommittedChangeSets().values()) {
      numCommittedChangeSets += changeIds.size();
    }
    visibilityUpperBound = txSnapshot.getVisibilityUpperBound();
  }
}
