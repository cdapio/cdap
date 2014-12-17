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

package co.cask.cdap.notifications;

/**
 * Retry policy object used by the {@link co.cask.cdap.notifications.NotificationContext#execute} method
 * to specify behavior when transaction execution fails.
 */
public abstract class TxRetryPolicy {

  /**
   * Failure policy option enum.
   */
  public enum Policy {
    /**
     * Retry operation after failure.
     */
    RETRY,

    /**
     * Drop operation after failure.
     */
    DROP
  }

  /**
   *
   * @param failures number of failures so far, including this one.
   * @param e exception that caused the failure.
   * @return a {@link Policy} code indicating what action should be done.
   */
  public abstract Policy handleFailure(int failures, Throwable e);

  /**
   * @param tries number of tries before dropping a transaction.
   * @return a {@link TxRetryPolicy} object that will drop after the given amount of {@code tries}.
   */
  public static TxRetryPolicy maxRetries(final int tries) {
    return new TxRetryPolicy() {
      @Override
      public Policy handleFailure(int failures, Throwable e) {
        if (failures >= tries) {
          return Policy.DROP;
        }
        return Policy.RETRY;
      }
    };
  }
}


