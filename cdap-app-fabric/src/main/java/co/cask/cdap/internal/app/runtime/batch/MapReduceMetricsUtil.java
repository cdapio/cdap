/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Utility class to get metrics report interval.
 */
public final class MapReduceMetricsUtil {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceMetricsUtil.class);

  private MapReduceMetricsUtil() {
  }

  public static long getReportIntervalMillis(CConfiguration cConf, Map<String, String> runtimeArgs) {
    int reportInterval = cConf.getInt(Constants.AppFabric.MAPREDUCE_STATUS_REPORT_INTERVAL_SECONDS);
    String val = runtimeArgs.get(Constants.AppFabric.MAPREDUCE_STATUS_REPORT_INTERVAL_SECONDS);
    if (val != null) {
      try {
        int interval = Integer.parseInt(val);
        if (interval < 1) {
          // note: this will be caught right below, to produce a similar message as an exception thrown by parseInt()
          throw new NumberFormatException("Must be at least 1");
        }
        reportInterval = interval;
      } catch (NumberFormatException e) {
        LOG.warn("Invalid value '{}' for '{}' given in runtime arguments: {}. Using default of {} seconds.",
                 val, Constants.AppFabric.MAPREDUCE_STATUS_REPORT_INTERVAL_SECONDS, e.getMessage(), reportInterval);
      }
    }
    return TimeUnit.SECONDS.toMillis(reportInterval);
  }
}
