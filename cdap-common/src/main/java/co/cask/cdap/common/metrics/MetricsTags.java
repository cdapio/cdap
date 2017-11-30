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

package co.cask.cdap.common.metrics;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.FlowletId;
import co.cask.cdap.proto.id.ServiceId;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

/**
 * Provides ways to obtain the tags for certain metrics.
 */
public final class MetricsTags {

  private MetricsTags() {
  }

  public static Map<String, String> contextMap(String... tags) {
    Preconditions.checkArgument(tags.length % 2 == 0);
    Map<String, String> tagMap = new HashMap<>();
    for (int i = 0; i < tags.length; i += 2) {
      tagMap.put(tags[i], tags[i + 1]);
    }
    return tagMap;
  }

  public static Map<String, String> flowlet(FlowletId flowletId) {
    return ImmutableMap.of(
      Constants.Metrics.Tag.NAMESPACE, flowletId.getNamespace(),
      Constants.Metrics.Tag.APP, flowletId.getApplication(),
      Constants.Metrics.Tag.FLOW, flowletId.getFlow(),
      Constants.Metrics.Tag.FLOWLET, flowletId.getFlowlet());
  }

  public static Map<String, String> service(ServiceId serviceId) {
    return ImmutableMap.of(
      Constants.Metrics.Tag.NAMESPACE, serviceId.getNamespace(),
      Constants.Metrics.Tag.APP, serviceId.getApplication(),
      Constants.Metrics.Tag.SERVICE, serviceId.getProgram());
  }

  public static Map<String, String> serviceHandler(ServiceId id, String handlerId) {
    return ImmutableMap.of(
      Constants.Metrics.Tag.NAMESPACE, id.getNamespace(),
      Constants.Metrics.Tag.APP, id.getApplication(),
      Constants.Metrics.Tag.SERVICE, id.getProgram(),
      Constants.Metrics.Tag.HANDLER, handlerId);
  }
}
