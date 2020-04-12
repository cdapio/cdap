/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.collect.Maps;
import com.google.inject.Injector;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.distributed.remote.RemoteMonitorType;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Authenticator;
import java.net.ProxySelector;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Utility class for runtime monitor.
 */
public final class RuntimeMonitors {

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeMonitors.class);

  /**
   * Creates a map from topic configuration name to the actual TMS topic based on the list of topic configuration names
   * specified by the {@link Constants.RuntimeMonitor#TOPICS_CONFIGS} key.
   */
  public static Map<String, String> createTopicConfigs(CConfiguration cConf) {
    return cConf.getTrimmedStringCollection(Constants.RuntimeMonitor.TOPICS_CONFIGS).stream().flatMap(key -> {
      int idx = key.lastIndexOf(':');
      if (idx < 0) {
        return Stream.of(Maps.immutableEntry(key, cConf.get(key)));
      }

      try {
        int totalTopicCount = Integer.parseInt(key.substring(idx + 1));
        if (totalTopicCount <= 0) {
          throw new IllegalArgumentException("Total topic number must be positive for system topic config '" +
                                               key + "'.");
        }
        // For metrics, We make an assumption that number of metrics topics on runtime are not different than
        // cdap system. So, we will add same number of topic configs as number of metrics topics so that we can
        // keep track of different offsets for each metrics topic.
        // TODO: CDAP-13303 - Handle different number of metrics topics between runtime and cdap system
        String topicPrefix = key.substring(0, idx);
        return IntStream
          .range(0, totalTopicCount)
          .mapToObj(i -> Maps.immutableEntry(topicPrefix + ":" + i, cConf.get(topicPrefix) + i));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Total topic number must be a positive number for system topic config'"
                                             + key + "'.", e);
      }
    }).sorted((o1, o2) -> {
      // Always put logs the last and program status event to the second to the last
      if (o1.getKey().startsWith(Constants.Logging.TMS_TOPIC_PREFIX)) {
        return 1;
      }
      if (o2.getKey().startsWith(Constants.Logging.TMS_TOPIC_PREFIX)) {
        return -1;
      }
      if (Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC.equals(o1.getKey())) {
        return 1;
      }
      if (Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC.equals(o2.getKey())) {
        return -1;
      }
      return o1.getKey().compareTo(o2.getKey());
    }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1, LinkedHashMap::new));
  }

  /**
   * Setups the monitoring routes and proxy for runtime monitoring.
   */
  public static void setupMonitoring(Injector injector, ProgramOptions programOpts) throws Exception {
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    RemoteMonitorType monitorType = injector.getInstance(RemoteMonitorType.class);
    if (monitorType == RemoteMonitorType.URL) {
      String provisioner = SystemArguments.getProfileProvisioner(programOpts.getArguments().asMap());
      String authenticatorKey = String.format("%s%s", Constants.RuntimeMonitor.MONITOR_URL_AUTHENTICATOR_CLASS_PREFIX,
                                              provisioner);
      Class<? extends RemoteAuthenticator> monitorAuthClass = cConf.getClass(authenticatorKey, null,
                                                                             RemoteAuthenticator.class);
      if (monitorAuthClass != null) {
        RemoteAuthenticator.setDefaultAuthenticator(monitorAuthClass.newInstance());
      }

      // This shouldn't be null, otherwise the type won't be URL.
      String monitorURL = cConf.get(Constants.RuntimeMonitor.MONITOR_URL);
      monitorURL = monitorURL.endsWith("/") ? monitorURL : monitorURL + "/";
      ProgramRunId programRunId = injector.getInstance(ProgramRunId.class);
      URI runtimeServiceBaseURI = URI.create(monitorURL).resolve(
        String.format("v3Internal/runtime/namespaces/%s/apps/%s/versions/%s/%s/%s/runs/%s/services/",
                      programRunId.getNamespace(), programRunId.getApplication(), programRunId.getVersion(),
                      programRunId.getType().getCategoryName(), programRunId.getProgram(),
                      programRunId.getRun()));
      System.setProperty(RemoteClient.RUNTIME_SERVICE_ROUTING_BASE_URI, runtimeServiceBaseURI.toString());

      LOG.debug("Setting runtime service routing base URI to {}", runtimeServiceBaseURI);
    } else {
      Authenticator.setDefault(injector.getInstance(Authenticator.class));
      ProxySelector.setDefault(injector.getInstance(ProxySelector.class));
    }
  }

  private RuntimeMonitors() {
    // no-op
  }
}
