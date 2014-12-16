package co.cask.cdap.notifications;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.notifications.client.AbstractNotificationSubscriber;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import com.google.common.base.Throwables;

/**
 * Implementation of {@link NotificationContext}.
 */
public final class BasicNotificationContext implements NotificationContext {

  private AbstractNotificationSubscriber abstractNotificationSubscriber;

  public BasicNotificationContext(AbstractNotificationSubscriber abstractNotificationSubscriber) {
    this.abstractNotificationSubscriber = abstractNotificationSubscriber;
  }

  @Override
  public boolean execute(TxRunnable runnable, TxRetryPolicy policy) {
    int countFail = 0;
    while (true) {
      try {
        final TransactionContext context = new TransactionContext(abstractNotificationSubscriber.transactionSystemClient);
        try {
          context.start();
          runnable.run(new AbstractNotificationSubscriber.DynamicDatasetContext(context));
          context.finish();
          return true;
        } catch (TransactionFailureException e) {
          abortTransaction(e, "Failed to commit. Aborting transaction.", context);
        } catch (Exception e) {
          abortTransaction(e, "Exception occurred running user code. Aborting transaction.", context);
        }
      } catch (Throwable t) {
        switch (policy.handleFailure(++countFail, t)) {
          case RETRY:
            AbstractNotificationSubscriber.LOG.warn("Retrying failed transaction");
            break;
          case DROP:
            AbstractNotificationSubscriber.LOG.warn("Could not execute transactional operation.", t);
            return false;
        }
      }
    }
  }

  private void abortTransaction(Exception e, String message, TransactionContext context) {
    try {
      AbstractNotificationSubscriber.LOG.error(message, e);
      context.abort();
      throw Throwables.propagate(e);
    } catch (TransactionFailureException e1) {
      AbstractNotificationSubscriber.LOG.error("Failed to abort transaction.", e1);
      throw Throwables.propagate(e1);
    }
  }
}
