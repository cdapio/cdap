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

package co.cask.cdap.api.log;

import java.util.Arrays;

import static co.cask.cdap.api.log.LogMarkerFilterList.Operator.MUST_PASS_ALL;
import static co.cask.cdap.api.log.LogMessage.LogLevel.INFO;

/**
 *
 */
public class LogServiceClientExample {
  public static void main(String[] args) throws UnsupportedLogQueryException, LogServiceException {
    LogService logService = null;
    long startTs = 0;
    long stopTs = 0;

  LogMarkerFilter markerFilter =
    new LogMarkerFilterList(MUST_PASS_ALL)
    .add(new ContainsMarkerFilter("special"))
    .add(new ContainsMarkerFilter("very_special"));

  LogQuery query = new LogQueryBuilder()
    .setAccountId("accountId")
    .setApplicationId("applicationId")
    .setFlowId("flowId")
    .setMinLogLevel(INFO)
    .setMarkerFilter(
                      new LogMarkerFilterList(MUST_PASS_ALL)
                        .add(new ContainsMarkerFilter("special"))
                        .add(new ContainsMarkerFilter("very_special"))
    )
    .build();

  for (LogMessage message : logService.query(query, startTs, stopTs)) {
    System.out.println(message.getTimestamp() + " " +
                       message.getLogLevel() + " " +
                       message.getText() + " " +
                       Arrays.toString(message.getMarkers())
    );
  }
  }
}
