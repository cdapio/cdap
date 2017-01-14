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

package co.cask.cdap.logging.framework;

import co.cask.cdap.proto.id.NamespaceId;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Helper class for getting logging file names from property map.
 */
public final class LoggingUtil {
  public static final String TAG_NAMESPACE_ID = ".namespaceId";
  public static final String TAG_APPLICATION_ID = ".applicationId";
  public static final String TAG_FLOW_ID = ".flowId";
  public static final String TAG_COMPONENT_ID = ".componentId";
  public static final String TAG_MAP_REDUCE_JOB_ID = ".mapReduceId";
  public static final String TAG_SPARK_JOB_ID = ".sparkId";
  public static final String TAG_USER_SERVICE_ID = ".userserviceid";
  public static final String TAG_WORKER_ID = ".workerid";
  public static final String TAG_WORKFLOW_ID = ".workflowId";
  public static final String SEPARATOR = ":";

  /**
   * From the MDC property map, determine the file name to be used for logging.
   * for Application : <app-name>:<program-type>:<program-name>:<timestamp>.log
   * for System service components : <system-service-name>:<timestamp>.log
   * @param propertyMap
   * @return
   */
  public static LogPathIdentifier getLoggingPath(Map<String, String> propertyMap) {
    // from the property map, get namespace values
    // if the namespace is system : get component-id and return that as path
    // if the namespace is non-system : get "app" and "program-name" and return that as path

    String namespaceId = propertyMap.get(TAG_NAMESPACE_ID);

    if (namespaceId.equals(NamespaceId.SYSTEM)) {
      // adding services to be consistent with the old format
      return new LogPathIdentifier(namespaceId, String.format(":services:%s", propertyMap.get(TAG_COMPONENT_ID)));
    } else {
      String path = propertyMap.get(TAG_APPLICATION_ID);

      List<String> programIdKeys = Arrays.asList(TAG_FLOW_ID, TAG_MAP_REDUCE_JOB_ID, TAG_SPARK_JOB_ID,
                                                 TAG_USER_SERVICE_ID, TAG_WORKER_ID, TAG_WORKFLOW_ID);

      for (String programId : programIdKeys) {
        if (propertyMap.containsKey(programId)) {
          path += SEPARATOR + propertyMap.get(programId);
          break;
        }
      }
      return new LogPathIdentifier(namespaceId, path);
    }
  }
}
