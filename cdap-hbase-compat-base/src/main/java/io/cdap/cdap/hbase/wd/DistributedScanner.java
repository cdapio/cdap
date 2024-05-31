/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
package io.cdap.cdap.hbase.wd;

import com.google.common.base.Throwables;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Interface for client-side scanning the data written with keys distribution
 */
public class DistributedScanner implements ResultScanner {

  private final AbstractRowKeyDistributor keyDistributor;
  private final ResultScanner[] scanners;
  private final List<Result>[] nextOfScanners;

  private final int caching;
  private final ExecutorService scansExecutor;

  private Result next;

  @SuppressWarnings("unchecked")
  private DistributedScanner(AbstractRowKeyDistributor keyDistributor,
      ResultScanner[] scanners,
      int caching,
      ExecutorService scansExecutor) {
    this.keyDistributor = keyDistributor;
    this.scanners = scanners;
    this.caching = caching;
    this.scansExecutor = scansExecutor;
    this.nextOfScanners = new List[scanners.length];
    for (int i = 0; i < this.nextOfScanners.length; i++) {
      this.nextOfScanners[i] = new ArrayList<>();
    }
  }

  private boolean hasNext() throws IOException {
    if (next != null) {
      return true;
    }

    next = nextInternal();

    return next != null;
  }

  @Override
  public Result next() throws IOException {
    if (hasNext()) {
      Result toReturn = next;
      next = null;
      return toReturn;
    }

    return null;
  }

  @Override
  public Result[] next(int nbRows) throws IOException {
    // Identical to HTable.ClientScanner implementation
    // Collect values to be returned here
    ArrayList<Result> resultSets = new ArrayList<>(nbRows);
    for (int i = 0; i < nbRows; i++) {
      Result next = next();
      if (next != null) {
        resultSets.add(next);
      } else {
        break;
      }
    }
    return resultSets.toArray(new Result[0]);
  }

  @Override
  public void close() {
    for (ResultScanner scanner : scanners) {
      scanner.close();
    }
  }

  public static DistributedScanner create(Table table,
      Scan originalScan,
      AbstractRowKeyDistributor keyDistributor,
      ExecutorService scansExecutor) throws IOException {
    Scan[] scans = keyDistributor.getDistributedScans(originalScan);

    ResultScanner[] rss = new ResultScanner[scans.length];
    for (int i = 0; i < scans.length; i++) {
      rss[i] = table.getScanner(scans[i]);
    }

    int caching = originalScan.getCaching();
    // to optimize work of distributed scan we need to know that, so we are resolving it from config in the case it is
    // not set for scan
    if (caching < 1) {
      caching = table.getConfiguration().getInt("hbase.client.scanner.caching", 1);
    }

    return new DistributedScanner(keyDistributor, rss, caching, scansExecutor);
  }

  private Result nextInternal() throws IOException {
    Result result = null;
    int indexOfScannerToUse = -1;

    // advancing scanners if needed in multi-threaded way
    Future[] advanceFutures = new Future[scanners.length];
    for (int i = 0; i < nextOfScanners.length; i++) {
      if (nextOfScanners[i] == null) {
        // result scanner is exhausted, don't advance it any more
        continue;
      }

      if (nextOfScanners[i].size() == 0) {
        final ResultScanner scanner = scanners[i];
        advanceFutures[i] = scansExecutor.submit(() -> scanner.next(caching));
      }
    }

    for (int i = 0; i < advanceFutures.length; i++) {
      @SuppressWarnings("unchecked")
      Future<Result[]> advanceFuture = advanceFutures[i];

      if (advanceFuture == null) {
        continue;
      }

      // advancing result scanner
      try {
        Result[] results = advanceFuture.get();
        if (results.length == 0) {
          // marking result scanner as exhausted
          nextOfScanners[i] = null;
        } else {
          nextOfScanners[i].addAll(Arrays.asList(results));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Scan thread interrupted", e);
      } catch (ExecutionException e) {
        Throwables.propagateIfPossible(e.getCause(), IOException.class);
        throw Throwables.propagate(e.getCause());
      }
    }

    for (int i = 0; i < nextOfScanners.length; i++) {
      if (nextOfScanners[i] == null) {
        // result scanner is exhausted, don't advance it any more
        continue;
      }

      // if result is null or next record has original key less than the candidate to be returned
      if (result == null
          || Bytes.compareTo(keyDistributor.getOriginalKey(nextOfScanners[i].get(0).getRow()),
          keyDistributor.getOriginalKey(result.getRow())) < 0) {
        result = nextOfScanners[i].get(0);
        indexOfScannerToUse = i;
      }
    }

    if (indexOfScannerToUse >= 0) {
      nextOfScanners[indexOfScannerToUse].remove(0);
    }

    return result;
  }

  @Override
  public Iterator<Result> iterator() {
    // Identical to HTable.ClientScanner implementation
    return new Iterator<Result>() {
      // The next RowResult, possibly pre-read
      Result next;

      // return true if there is another item pending, false if there isn't.
      // this method is where the actual advancing takes place, but you need
      // to call next() to consume it. hasNext() will only advance if there
      // isn't a pending next().
      @Override
      public boolean hasNext() {
        if (next == null) {
          try {
            next = DistributedScanner.this.next();
            return next != null;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return true;
      }

      // get the pending next item and advance the iterator. returns null if
      // there is no next item.
      @Override
      public Result next() {
        // since hasNext() does the real advancing, we call this to determine
        // if there is a next before proceeding.
        if (!hasNext()) {
          return null;
        }

        // if we get to here, then hasNext() has given us an item to return.
        // we want to return the item and then null out the next pointer, so
        // we use a temporary variable.
        Result temp = next;
        next = null;
        return temp;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
