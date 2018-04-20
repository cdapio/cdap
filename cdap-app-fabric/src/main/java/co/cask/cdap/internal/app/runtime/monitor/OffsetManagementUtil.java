/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.monitor;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.store.AppMetadataStore;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Util to manage offsets for runtime(s).
 */
public class OffsetManagementUtil {
  private static final String SEPERATOR = ":r:";

  /**
   *
   * @param cConf
   * @param dsFramework
   * @param context
   * @param initialOffsets
   * @param programRunId
   * @param topicConfigs
   * @throws Exception
   */
  public static void initializeOffsets(CConfiguration cConf, DatasetFramework dsFramework, DatasetContext context,
                                       Map<String, MonitorConsumeRequest> initialOffsets, String programRunId,
                                       Set<String> topicConfigs) throws Exception {
    int limit = cConf.getInt(Constants.RuntimeMonitor.BATCH_LIMIT);

    for (String topicConfig : topicConfigs) {
      String messageId = getAppMetadataStore(cConf, dsFramework, context).retrieveSubscriberState(
        getRowKey(programRunId, topicConfig), null);

      initialOffsets.put(topicConfig, new MonitorConsumeRequest(messageId, limit));
    }
  }

  /**
   *
   * @param cConf
   * @param dsFramework
   * @param context
   * @param programRunId
   * @param topicConfig
   * @param messageId
   * @throws Exception
   */
  public static void persistOffsets(CConfiguration cConf, DatasetFramework dsFramework, DatasetContext context,
                                    String programRunId, String topicConfig, String messageId) throws Exception {
    getAppMetadataStore(cConf, dsFramework, context).persistSubscriberState(getRowKey(programRunId, topicConfig),
                                                                            null, messageId);
  }

  /**
   *
   * @param cConf
   * @param dsFramework
   * @param context
   * @param programRunId
   * @throws Exception
   */
  public static void cleanupOffsets(CConfiguration cConf, DatasetFramework dsFramework, DatasetContext context,
                                    String programRunId) throws Exception {
    getAppMetadataStore(cConf, dsFramework, context).deleteSubscriberState(getRowKeyPrefix(programRunId), null);
  }

  /**
   *
   */
  private static AppMetadataStore getAppMetadataStore(CConfiguration cConf, DatasetFramework dsFramework,
                                                      DatasetContext datasetContext)
    throws IOException, DatasetManagementException {
    return AppMetadataStore.create(cConf, datasetContext, dsFramework);
  }

  /**
   * Constructs a row key using program run id and topic config
   */
  private static String getRowKey(String programRunId, String topicConfig) {
    return programRunId + SEPERATOR + topicConfig;
  }

  /**
   * Provides row key prefix for a program run
   */
  private static String getRowKeyPrefix(String programRunId) {
    return programRunId + SEPERATOR;
  }
}
