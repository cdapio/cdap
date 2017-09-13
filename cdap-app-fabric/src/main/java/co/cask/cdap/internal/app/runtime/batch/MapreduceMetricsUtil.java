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

import java.util.Map;

/**
 * Utility class to determine if mapreduce job should emit task level metrics
 */
public final class MapreduceMetricsUtil {

  private MapreduceMetricsUtil() {
  }

  /**
   * If runtime argument is provided by user for the property {@link Constants.Metrics#EMIT_MR_TASKL_LEVEL_METRICS},
   * that will take the precedence, else we get the system level
   * configuration provided for the property {@link Constants.Metrics#EMIT_MR_TASKL_LEVEL_METRICS}
   * @return true if emitting task level metrics are enabled
   */
  static boolean isTaskLevelMetricsEnabled(Map<String, String> runtimeArgs, CConfiguration cConf) {
    String emitMetricsPreference = runtimeArgs.get(Constants.Metrics.EMIT_MR_TASKL_LEVEL_METRICS);
    return emitMetricsPreference != null ? Boolean.valueOf(emitMetricsPreference) :
      cConf.getBoolean(Constants.Metrics.EMIT_MR_TASKL_LEVEL_METRICS);
  }
}
