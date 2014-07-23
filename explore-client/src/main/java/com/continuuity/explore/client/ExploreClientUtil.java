/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.explore.client;

import com.continuuity.explore.service.Explore;
import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.HandleNotFoundException;
import com.continuuity.proto.QueryHandle;
import com.continuuity.proto.QueryStatus;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Helper methods for explore client.
 */
public class ExploreClientUtil {

  /**
   * Polls for state of the operation represented by the {@link QueryHandle}, and returns when operation has completed
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
  public static QueryStatus waitForCompletionStatus(Explore exploreClient, QueryHandle handle,
                                               long sleepTime, TimeUnit timeUnit, int maxTries)
    throws ExploreException, HandleNotFoundException, InterruptedException, SQLException {
    QueryStatus status;
    int tries = 0;
    do {
      timeUnit.sleep(sleepTime);
      status = exploreClient.getStatus(handle);

      if (++tries > maxTries) {
        break;
      }
    } while (status.getStatus() == QueryStatus.OpStatus.RUNNING ||
             status.getStatus() == QueryStatus.OpStatus.PENDING ||
             status.getStatus() == QueryStatus.OpStatus.INITIALIZED ||
             status.getStatus() == QueryStatus.OpStatus.UNKNOWN);
    return status;
  }
}
