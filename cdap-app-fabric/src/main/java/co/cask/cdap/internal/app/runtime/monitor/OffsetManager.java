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

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
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
 * Manages offsets for runtime.
 */
public class OffsetManager {
  private static final String HAS_FINISHED = "f";
  private static final String SEPERATOR = ":r:";
  private final CConfiguration cConf;
  private final DatasetFramework dsFramework;
  private Transactional transactional;

  public OffsetManager(DatasetFramework dsFramework, Transactional transactional, CConfiguration cConf) {
    this.cConf = cConf;
    this.dsFramework = dsFramework;
    this.transactional = transactional;
  }

  public void initializeOffsets(Map<String, MonitorConsumeRequest> topicsToRequest, String programId,
                                Set<String> topicConfigs) throws Exception {
    int limit = cConf.getInt(Constants.RuntimeMonitor.BATCH_LIMIT);

    for (String topicConfig : topicConfigs) {
      Transactionals.execute(transactional, context -> {
        String messageId = getAppMetadataStore(context).retrieveSubscriberState(getRowKey(programId, topicConfig),
                                                                                null);
        topicsToRequest.put(topicConfig, new MonitorConsumeRequest(messageId, limit));
      });
    }
  }

  public void persistOffsets(String programId, String topicConfig, String messageId) throws Exception {
    Transactionals.execute(transactional, context -> {
      getAppMetadataStore(context).persistSubscriberState(getRowKey(programId, topicConfig), null, messageId);
    });
  }

  public void cleanupOffsets(String programId) throws Exception {
    Transactionals.execute(transactional, context -> {
      getAppMetadataStore(context).deleteSubscriberState(getRowKeyPrefix(programId), null);
    });
  }

  public void runFinished(String programId) throws Exception {
    Transactionals.execute(transactional, context -> {
      getAppMetadataStore(context).persistSubscriberState(getRowKey(programId, HAS_FINISHED), null, HAS_FINISHED);
    });
  }

  public boolean hasRunFinished(String programId) throws Exception {
    return Transactionals.execute(transactional, context -> {
      String message = getAppMetadataStore(context).retrieveSubscriberState(getRowKey(programId, HAS_FINISHED), null);
      return message != null;
    });
  }

  private AppMetadataStore getAppMetadataStore(DatasetContext datasetContext)
    throws IOException, DatasetManagementException {
    return AppMetadataStore.create(cConf, datasetContext, dsFramework);
  }

  private String getRowKey(String programId, String topicConfig) {
    return programId + SEPERATOR + topicConfig;
  }

  private String getRowKeyPrefix(String programId) {
    return programId + SEPERATOR;
  }
}
