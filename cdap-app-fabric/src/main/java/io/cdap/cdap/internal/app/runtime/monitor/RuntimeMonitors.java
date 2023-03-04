/*
 * Copyright © 2020-2022 Cask Data, Inc.
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
import com.google.inject.Module;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.RuntimeMonitorType;
import java.net.Authenticator;
import java.net.ProxySelector;
import java.net.URI;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for runtime monitor.
 */
public final class RuntimeMonitors {

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeMonitors.class);

  /**
   * Creates a properly ordered list of TMS topic names based on the list of topic configuration
   * names specified by the {@link Constants.RuntimeMonitor#TOPICS_CONFIGS} key.
   */
  public static List<String> createTopicNameList(CConfiguration cConf) {
    return cConf.getTrimmedStringCollection(Constants.RuntimeMonitor.TOPICS_CONFIGS).stream()
        .flatMap(key -> {
          int idx = key.lastIndexOf(':');
          if (idx < 0) {
            return Stream.of(Maps.immutableEntry(key, cConf.get(key)));
          }

          try {
            int totalTopicCount = Integer.parseInt(key.substring(idx + 1));
            if (totalTopicCount <= 0) {
              throw new IllegalArgumentException(
                  "Total topic number must be positive for system topic config '"
                      + key + "'.");
            }
            // For metrics, We make an assumption that number of metrics topics on runtime are not different than
            // cdap system. So, we will add same number of topic configs as number of metrics topics so that we can
            // keep track of different offsets for each metrics topic.
            // TODO: CDAP-13303 - Handle different number of metrics topics between runtime and cdap system
            String topicPrefix = key.substring(0, idx);
            return IntStream
                .range(0, totalTopicCount)
                .mapToObj(
                    i -> Maps.immutableEntry(topicPrefix + ":" + i, cConf.get(topicPrefix) + i));
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Total topic number must be a positive number for system topic config'"
                    + key + "'.", e);
          }
        }).sorted(Comparator.comparing(topic ->
            // Always put program status event to the last
            // Logs to the second to the last
            topic.getKey().startsWith(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC) ? 2 :
                topic.getKey().startsWith(Constants.Logging.TMS_TOPIC_PREFIX) ? 1 :
                    0
        )).map(e -> e.getValue()).collect(Collectors.toList());
  }

  /**
   * Setups the monitoring routes and proxy for runtime monitoring.
   */
  public static void setupMonitoring(Injector injector, ProgramOptions programOpts)
      throws Exception {
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    RuntimeMonitorType monitorType = injector.getInstance(RuntimeMonitorType.class);
    if (monitorType == RuntimeMonitorType.URL) {
      // This shouldn't be null, otherwise the type won't be URL.
      String monitorURL = cConf.get(Constants.RuntimeMonitor.MONITOR_URL);
      monitorURL = monitorURL.endsWith("/") ? monitorURL : monitorURL + "/";

      ProgramRunId programRunId = injector.getInstance(ProgramRunId.class);
      WorkflowProgramInfo workflowInfo = WorkflowProgramInfo.create(programOpts.getArguments());
      if (workflowInfo != null) {
        // If the program is launched by Workflow, use the Workflow run id to make service request.
        programRunId = new ProgramRunId(programRunId.getNamespace(), programRunId.getApplication(),
            ProgramType.WORKFLOW, workflowInfo.getName(), workflowInfo.getRunId().getId());
      }

      URI runtimeServiceBaseURI = URI.create(monitorURL).resolve(
          String.format(
              "v3Internal/runtime/namespaces/%s/apps/%s/versions/%s/%s/%s/runs/%s/services/",
              programRunId.getNamespace(), programRunId.getApplication(), programRunId.getVersion(),
              programRunId.getType().getCategoryName(), programRunId.getProgram(),
              programRunId.getRun()));
      System.setProperty(RemoteClient.RUNTIME_SERVICE_ROUTING_BASE_URI,
          runtimeServiceBaseURI.toString());

      LOG.debug("Setting runtime service routing base URI to {}", runtimeServiceBaseURI);
    } else {
      Authenticator.setDefault(injector.getInstance(Authenticator.class));
      ProxySelector.setDefault(injector.getInstance(ProxySelector.class));
    }
  }

  /**
   * Returns a trimmed system arg map to be used for creating {@link RunRecordDetail} This is used
   * for removing unnecessary information in system args stored in {@link RunRecordDetail}, thus
   * minimizing its storage and processing overhead.
   */
  public static Map<String, String> trimSystemArgs(Map<String, String> args) {
    Map<String, String> trimmed = new HashMap<>();
    if (args.containsKey(SystemArguments.PROFILE_NAME)) {
      trimmed.put(SystemArguments.PROFILE_NAME, args.get(SystemArguments.PROFILE_NAME));
    }
    if (args.containsKey(ProgramOptionConstants.PRINCIPAL)) {
      trimmed.put(ProgramOptionConstants.PRINCIPAL, args.get(ProgramOptionConstants.PRINCIPAL));
    }
    return trimmed;
  }

  private RuntimeMonitors() {
    // no-op
  }

  /**
   * Returns a module which defines remote authenticator override bindings if runtime monitoring
   * type is URL, otherwise returns a null provider.
   */
  public static Module getRemoteAuthenticatorModule(RuntimeMonitorType runtimeMonitorType,
      ProgramOptions programOpts) {
    // Module for remote authenticator from overridden config if using URL runtime monitoring.
    String remoteAuthenticatorNameKey = Constants.RemoteAuthenticator.REMOTE_AUTHENTICATOR_NAME;
    if (runtimeMonitorType == RuntimeMonitorType.URL) {
      String provisioner = SystemArguments.getProfileProvisioner(
          programOpts.getArguments().asMap());
      remoteAuthenticatorNameKey = String.format("%s%s",
          Constants.RuntimeMonitor.MONITOR_URL_AUTHENTICATOR_NAME_PREFIX,
          provisioner);
    }
    return RemoteAuthenticatorModules.getDefaultModule(remoteAuthenticatorNameKey);
  }
}
