package com.continuuity.common.backoff;

import java.io.IOException;

/**
 * A strategy interface to control back off between retry attempts.
 */
public interface BackoffPolicy {

  /**
   * Value indicating that no more retries should be made,
   * see {@link #getNextBackOffMillis()}
   */
  public static final long STOP = -1L;

  /**
   * Determines if back off is required based on the specified object.
   *
   * <p>
   *   Implementations may want to back off on server or product specific
   *   errors.
   * </p>
   * @param object that provide information to make decision of back-off
   * @return
   */
  public boolean isBackOffRequired(Object object);

  /**
   * Reset back off counters (if any) in an implementation specific fashion.
   */
  public void reset();


  /**
   * Gets the number of milliseconds to wait before retrying a failed
   * request. If {@link #STOP} is returned, no retries should be made.
   *
   * This method should be used as follows:
   *
   * <pre>
   *   long backoffTime = backoffPolicy.getNextBackOffMillios();
   *   if(backoffTime == BackoffPolicy.STOP) {
   *     // Stop retrying.
   *   } else {
   *     // Retry after backoffTime.
   *   }
   * </pre>
   * @return  number of milliseconds to wait when backing off requests, or
   * {@link #STOP} if no more retries should be made.
   *
   * @throws IOException
   */
  public long getNextBackOffMillis() throws IOException;

}
