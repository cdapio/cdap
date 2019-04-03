/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.operations.cdap;

import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.operations.AbstractOperationalStats;
import co.cask.cdap.operations.OperationalStats;
import com.google.common.annotations.VisibleForTesting;

/**
 * Base class for CDAP {@link OperationalStats}.
 */
public abstract class AbstractCDAPStats extends AbstractOperationalStats {
  @VisibleForTesting
  static final String SERVICE_NAME = "CDAP";

  @Override
  public String getServiceName() {
    return SERVICE_NAME;
  }

  protected long aggregateMetricValue(MetricTimeSeries metricTimeSery) {
    long aggregateValue = 0L;
    for (TimeValue timeValue : metricTimeSery.getTimeValues()) {
      aggregateValue += timeValue.getValue();
    }
    return aggregateValue;
  }
}
