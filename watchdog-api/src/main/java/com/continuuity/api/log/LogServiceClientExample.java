/*
 * com.continuuity - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.log;

import java.util.Arrays;

import static com.continuuity.api.log.LogMarkerFilterList.Operator.MUST_PASS_ALL;
import static com.continuuity.api.log.LogMessage.LogLevel.INFO;

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
