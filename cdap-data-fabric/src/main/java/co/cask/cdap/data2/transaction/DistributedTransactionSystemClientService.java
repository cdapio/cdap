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

package co.cask.cdap.data2.transaction;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.tephra.InvalidTruncateTimeException;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionCouldNotTakeSnapshotException;
import org.apache.tephra.TransactionNotInProgressException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A Service for Tephra's TransactionSystemClient that waits on startup for the transaction system to be available.
 * Everything else is just delegated to a TransactionSystemClient.
 */
public class DistributedTransactionSystemClientService
  extends AbstractIdleService implements TransactionSystemClientService {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedTransactionSystemClientService.class);
  private final CConfiguration cConf;
  private final TransactionSystemClient delegate;
  private final DiscoveryServiceClient discoveryServiceClient;

  @Inject
  public DistributedTransactionSystemClientService(CConfiguration cConf,
                                                   DiscoveryServiceClient discoveryServiceClient,
                                                   TransactionSystemClient delegate) {
    this.cConf = cConf;
    this.delegate = delegate;
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting TransactionSystemClientService.");
    int timeout = cConf.getInt(Constants.Startup.STARTUP_SERVICE_TIMEOUT);
    if (timeout > 0) {
      LOG.debug("Waiting for transaction system to be available. Will timeout after {} seconds.", timeout);
      try {
        Tasks.waitFor(true, new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            return discoveryServiceClient.discover(Constants.Service.TRANSACTION).iterator().hasNext();
          }
        }, timeout, TimeUnit.SECONDS, Math.min(timeout, Math.max(10, timeout / 10)), TimeUnit.SECONDS);
        LOG.info("TransactionSystemClientService started.");
      } catch (TimeoutException e) {
        // its not a nice message... throw one with a better message
        throw new TimeoutException(String.format(
          "Timed out after %d seconds while waiting to discover the %s service. " +
            "Check the logs for the service to see what went wrong.",
          timeout, Constants.Service.TRANSACTION));
      } catch (InterruptedException e) {
        throw new RuntimeException(String.format("Interrupted while waiting to discover the %s service.",
                                                 Constants.Service.TRANSACTION));
      } catch (ExecutionException e) {
        throw new RuntimeException(String.format("Error while waiting to discover the %s service.",
                                                 Constants.Service.TRANSACTION), e);
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    // no-op
  }

  @Override
  public Transaction startShort() {
    return delegate.startShort();
  }

  @Override
  public Transaction startShort(int timeout) {
    return delegate.startShort(timeout);
  }

  @Override
  public Transaction startLong() {
    return delegate.startLong();
  }

  @Override
  public boolean canCommit(Transaction tx, Collection<byte[]> changeIds) throws TransactionNotInProgressException {
    return delegate.canCommit(tx, changeIds);
  }

  @Override
  public boolean commit(Transaction tx) throws TransactionNotInProgressException {
    return delegate.commit(tx);
  }

  @Override
  public void abort(Transaction tx) {
    delegate.abort(tx);
  }

  @Override
  public boolean invalidate(long tx) {
    return delegate.invalidate(tx);
  }

  @Override
  public Transaction checkpoint(Transaction tx) throws TransactionNotInProgressException {
    return delegate.checkpoint(tx);
  }

  @Override
  public InputStream getSnapshotInputStream() throws TransactionCouldNotTakeSnapshotException {
    return delegate.getSnapshotInputStream();
  }

  @Override
  public String status() {
    return delegate.status();
  }

  @Override
  public void resetState() {
    delegate.resetState();
  }

  @Override
  public boolean truncateInvalidTx(Set<Long> invalidTxIds) {
    return delegate.truncateInvalidTx(invalidTxIds);
  }

  @Override
  public boolean truncateInvalidTxBefore(long time) throws InvalidTruncateTimeException {
    return delegate.truncateInvalidTxBefore(time);
  }

  @Override
  public int getInvalidSize() {
    return delegate.getInvalidSize();
  }
}
