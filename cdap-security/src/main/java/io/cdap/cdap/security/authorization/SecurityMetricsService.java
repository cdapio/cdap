/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.security.authorization;

import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.metrics.NoopMetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.metrics.ProgramTypeMetricTag;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A helper class to inject metric service and other common metric functions into APIs.
 */
public class SecurityMetricsService {
  private final boolean metricsCollectionEnabled;
  private final boolean metricsTagsEnabled;
  private final MetricsCollectionService metricsCollectionService;

  @Inject
  public SecurityMetricsService(CConfiguration cConf, MetricsCollectionService metricsCollectionService) {
    this.metricsCollectionEnabled = cConf.getBoolean(
      Constants.Metrics.AUTHORIZATION_METRICS_ENABLED, false);
    this.metricsTagsEnabled = cConf.getBoolean(Constants.Metrics.AUTHORIZATION_METRICS_TAGS_ENABLED,
                                               false);
    this.metricsCollectionService = metricsCollectionService;
  }

  /**
   * Constructs tags and returns a metrics context for a given entity ID.
   */
  public MetricsContext createEntityIdMetricsContext(EntityId entityId) {
    if (!metricsCollectionEnabled) {
      return new NoopMetricsContext();
    }
    Map<String, String> tags = Collections.emptyMap();
    if (metricsTagsEnabled && entityId != null) {
      tags = createEntityIdMetricsTags(entityId);
    }
    return metricsCollectionService == null ? new NoopMetricsContext(tags)
      : metricsCollectionService.getContext(tags);
  }

  /**
   * Constructs metrics tags for a given entity ID.
   */
  static Map<String, String> createEntityIdMetricsTags(EntityId entityId) {
    Map<String, String> tags = new HashMap<>();
    for (EntityId currEntityId : entityId.getHierarchy()) {
      addTagsForEntityId(tags, currEntityId);
    }
    return tags;
  }

  private static void addTagsForEntityId(Map<String, String> tags, EntityId entityId) {
    switch (entityId.getEntityType()) {
      case INSTANCE:
        tags.put(Constants.Metrics.Tag.INSTANCE_ID, entityId.getEntityName());
        break;
      case NAMESPACE:
        tags.put(Constants.Metrics.Tag.NAMESPACE, entityId.getEntityName());
        break;
      case PROGRAM_RUN:
        ProgramRunId programRunId = (ProgramRunId) entityId;
        tags.put(Constants.Metrics.Tag.RUN_ID, entityId.getEntityName());
        tags.put(ProgramTypeMetricTag.getTagName(programRunId.getType()),
                 programRunId.getProgram());
        break;
      case DATASET:
        tags.put(Constants.Metrics.Tag.DATASET, entityId.getEntityName());
        break;
      case APPLICATION:
        tags.put(Constants.Metrics.Tag.APP, entityId.getEntityName());
        break;
      case PROGRAM:
        ProgramId programId = (ProgramId) entityId;
        tags.put(Constants.Metrics.Tag.PROGRAM, programId.getProgram());
        tags.put(Constants.Metrics.Tag.PROGRAM_TYPE,
                 ProgramTypeMetricTag.getTagName(programId.getType()));
        break;
      case PROFILE:
        tags.put(Constants.Metrics.Tag.PROFILE, entityId.getEntityName());
        break;
      case OPERATION_RUN:
        tags.put(Constants.Metrics.Tag.OPERATION_RUN, entityId.getEntityName());
        break;
      default:
        // No tags to set
    }
  }
}

