/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.storage.spanner;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TxRunnable;

/**
 * A {@link TransactionRunner} implemented by using Google Cloud Spanner transaction.
 */
public class SpannerTransactionRunner implements TransactionRunner {

  private final SpannerStructuredTableAdmin admin;

  public SpannerTransactionRunner(SpannerStructuredTableAdmin admin) {
    this.admin = admin;
  }

  @Override
  public void run(TxRunnable runnable) throws TransactionException {
    try {
      admin.getDatabaseClient().readWriteTransaction().allowNestedTransaction().run(context -> {
        runnable.run(tableId -> new SpannerStructuredTable(context, admin.getSchema(tableId)));
        return null;
      });
    } catch (SpannerException e) {
      // If the runnable.run throws, Spanner wrap it with UNKNOWN error code. We unwrap it so that
      // the TransactionRunners can inspect the cause correctly.
      if (e.getErrorCode() == ErrorCode.UNKNOWN) {
        throw new TransactionException("Exception raised by TxRunnable", e.getCause());
      }
      throw new TransactionException("Exception raised in Spanner operation", e);
    } catch (Exception e) {
      throw new TransactionException("Failed to execute TxRunnable", e);
    }
  }
}
