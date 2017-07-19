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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.api.common.Bytes;
import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionCodec;
import org.apache.tephra.TransactionFailureException;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * Client class to interact with {@link SparkTransactionHandler} through HTTP. It is used by tasks executed inside
 * executor processes.
 */
public final class SparkTransactionClient {

  private static final TransactionCodec TX_CODEC = new TransactionCodec();
  private static final long DEFAULT_TX_POLL_INTERVAL_MS = 50;

  private final URI txServiceBaseURI;
  private final long txPollIntervalMillis;

  public SparkTransactionClient(URI txServiceBaseURI) {
    this(txServiceBaseURI, DEFAULT_TX_POLL_INTERVAL_MS);
  }

  public SparkTransactionClient(URI txServiceBaseURI, long txPollIntervalMillis) {
    this.txServiceBaseURI = txServiceBaseURI;
    this.txPollIntervalMillis = txPollIntervalMillis;
  }

  /**
   * Returns the {@link Transaction} for the given stage.
   *
   * @param stageId the stage id to query for {@link Transaction}.
   * @param timeout the maximum time to wait
   * @param timeUnit the time unit of the timeout argument
   * @return the {@link Transaction} to be used for the given stage.
   *
   * @throws TimeoutException if the wait timed out
   * @throws InterruptedException if the current thread was interrupted while waiting
   * @throws TransactionFailureException if failed to get transaction for the given stage. Calling this method again
   *                                     with the same stage id will result in the same exception
   */
  public Transaction getTransaction(int stageId, long timeout,
                                    TimeUnit timeUnit) throws TimeoutException, InterruptedException,
                                                       TransactionFailureException {
    long timeoutMillis = Math.max(0L, timeUnit.toMillis(timeout) - txPollIntervalMillis);
    Stopwatch stopwatch = new Stopwatch().start();
    Transaction transaction = getTransaction(stageId);

    while (transaction == null && stopwatch.elapsedMillis() < timeoutMillis) {
      TimeUnit.MILLISECONDS.sleep(txPollIntervalMillis);
      transaction = getTransaction(stageId);
    }
    if (transaction == null) {
      throw new TimeoutException("Cannot get transaction for stage " + stageId + " after " + timeout + " " + timeUnit);
    }
    return transaction;
  }

  @Nullable
  private Transaction getTransaction(int stageId) throws TransactionFailureException {
    try {
      URL url = txServiceBaseURI.resolve("/spark/stages/" + stageId + "/transaction").toURL();
      HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
      try {
        int responseCode = urlConn.getResponseCode();
        if (responseCode == 200) {
          return TX_CODEC.decode(ByteStreams.toByteArray(urlConn.getInputStream()));
        }
        if (responseCode == 404) {
          return null;
        }
        throw new TransactionFailureException(
          String.format("No transaction for stage %d. Reason: %s", stageId,
                        Bytes.toString(ByteStreams.toByteArray(urlConn.getErrorStream()))));
      } finally {
        urlConn.disconnect();
      }
    } catch (IOException e) {
      // If not able to talk to the tx service, just treat it the same as 404 so that there could be retry.
      return null;
    }
  }
}
