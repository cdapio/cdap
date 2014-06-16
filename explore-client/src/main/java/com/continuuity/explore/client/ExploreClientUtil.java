package com.continuuity.explore.client;

import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.HandleNotFoundException;
import com.continuuity.explore.service.Status;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ExploreClientUtil {

  /**
   * Polls for state of the operation represented by the {@link Handle}, and returns when operation has completed
   * execution.
   * @param exploreClient explore client used to poll status.
   * @param handle handle representing the operation.
   * @param sleepTime time to sleep between pooling.
   * @param timeUnit unit of sleepTime.
   * @param maxTries max number of times to poll.
   * @return completion status of the operation, null on reaching maxTries.
   * @throws ExploreException
   * @throws HandleNotFoundException
   * @throws InterruptedException
   */
  public static Status waitForCompletionStatus(ExploreClient exploreClient, Handle handle,
                                                long sleepTime, TimeUnit timeUnit, int maxTries)
    throws ExploreException, HandleNotFoundException, InterruptedException {
    Status status;
    int tries = 0;
    do {
      timeUnit.sleep(sleepTime);
      status = exploreClient.getStatus(handle);

      if (++tries > maxTries) {
        break;
      }
    } while (status.getState() == Status.State.RUNNING || status.getState() == Status.State.PENDING ||
      status.getState() == Status.State.INITIALIZED || status.getState() == Status.State.UNKNOWN);
    return status;
  }
}
