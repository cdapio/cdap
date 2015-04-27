/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.app.metrics;

import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.common.conf.Constants;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Utilities for flow metrics.
 */
public class FlowMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(FlowMetrics.class);

  /**
   * Delete the "system.queue.pending" metrics for a flow or for all flows in an app or a namespace.
   *
   * @param namespace the namespace id; may only be null if the appId and flowId are null
   * @param appId the application id; may only be null if the flowId is null
   */
  public static void deleteFlowPendingMetrics(MetricStore metricStore,
                                              @Nullable String namespace,
                                              @Nullable String appId,
                                              @Nullable String flowId)
    throws Exception {
    Preconditions.checkArgument(namespace != null || appId == null, "Namespace may only be null if AppId is null");
    Preconditions.checkArgument(appId != null || flowId == null, "AppId may only be null if FlowId is null");
    Collection<String> names = Collections.singleton("system.queue.pending");
    Map<String, String> tags = Maps.newHashMap();
    if (namespace != null) {
      tags.put(Constants.Metrics.Tag.NAMESPACE, namespace);
      if (appId != null) {
        tags.put(Constants.Metrics.Tag.APP, appId);
        if (flowId != null) {
          tags.put(Constants.Metrics.Tag.FLOW, flowId);
        }
      }
    }
    LOG.info("Deleting 'system.queue.pending' metric for context " + tags);
    // we must delete up to the current time - let's round up to the next second.
    long nextSecond = System.currentTimeMillis() / 1000 + 1;
    metricStore.delete(new MetricDeleteQuery(0L, nextSecond, names, tags));
  }


}
