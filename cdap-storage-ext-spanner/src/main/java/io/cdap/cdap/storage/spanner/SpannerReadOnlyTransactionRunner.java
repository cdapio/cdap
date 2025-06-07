package io.cdap.cdap.storage.spanner;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.TransactionContext;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TxRunnable;

public class SpannerReadOnlyTransactionRunner implements TransactionRunner {
  private final SpannerStructuredTableAdmin admin;

  public SpannerReadOnlyTransactionRunner(SpannerStructuredTableAdmin admin) {
    this.admin = admin;
  }

  @Override
  public void run(TxRunnable runnable) throws TransactionException {

    try (ReadOnlyTransaction tx = admin.getDatabaseClient().readOnlyTransaction()) {
      runnable.run(tableId -> new SpannerStructuredTable(new ReadOnlyTransactionContext(tx), admin.getSpannerStructuredTableSchema(tableId)));
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
