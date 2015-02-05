/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.metrics.query;

import co.cask.cdap.common.conf.Constants;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Class representing the context for a metrics request, including whether or not there was a tag associated
 * with the request.
 */
public class MetricsRequestContext {
  private final String namespaceId;
  private final String typeId;
  private final String requestId;
  private final String componentId;
  private final String contextPrefix;
  private final TagType tagType;
  private final String tag;
  private final MetricsRequestParser.RequestType requestType;
  private final MetricsRequestParser.PathType pathType;
  private final String runId;

  /**
   * Represents the tag type for metrics context.
   */
  public enum TagType {
    STREAM,
    DATASET,
    SERVICE,
    QUEUE
  }

  private MetricsRequestContext(String namespaceId, String typeId, MetricsRequestParser.PathType pathType,
                                MetricsRequestParser.RequestType requestType,
                                String requestId, String componentId, TagType tagType, String tag, String runId) {
    this.namespaceId = namespaceId;
    this.typeId = typeId;
    this.pathType = pathType;
    this.requestType = requestType;
    this.requestId = requestId;
    this.componentId = componentId;
    this.tagType = tagType;
    this.tag = tag;
    this.runId = runId;

    List<String> contextParts = Lists.newArrayListWithCapacity(4);
    // the first part of the context is namespaceId. If it is null, set contextPrefix to null
    if (namespaceId == null || namespaceId.isEmpty()) {
      this.contextPrefix = null;
    } else {
      contextParts.add(namespaceId);
      if (typeId != null && !typeId.isEmpty()) {
        contextParts.add(typeId);
        if (requestType != null) {
          if (!requestType.equals(MetricsRequestParser.RequestType.HANDLERS)) {
            contextParts.add(requestType.getCode());
          }
          if (requestId != null && !requestId.isEmpty()) {
            contextParts.add(requestId);
            if (componentId != null && !componentId.isEmpty()) {
              contextParts.add(componentId);
            }
          }
        }
      }
      this.contextPrefix = Joiner.on(".").join(contextParts);
    }
  }

  public String getTypeId() {
    return typeId;
  }

  public String getRequestId() {
    return requestId;
  }

  public MetricsRequestParser.RequestType getRequestType() {
    return requestType;
  }

  public MetricsRequestParser.PathType getPathType() {
    return pathType;
  }

  public String getComponentId() {
    return componentId;
  }

  public String getContextPrefix() {
    return contextPrefix;
  }

  public TagType getTagType() {
    return tagType;
  }

  public String getTag() {
    return tag;
  }

  public String getRunId() {
    return runId;
  }

  /**
   * Builds a metrics context.
   */
  public static class Builder {
    private String namespaceId;
    private String typeId;
    private String requestId;
    private String componentId;
    private TagType tagType;
    private String tag;
    private String runId;
    private MetricsRequestParser.RequestType requestType;
    private MetricsRequestParser.PathType pathType;

    public Builder setNamespaceId(String namespaceId) {
      this.namespaceId = namespaceId;
      return this;
    }

    public Builder setTypeId(String typeId) {
      this.typeId = typeId;
      return this;
    }

    public Builder setRequestType(MetricsRequestParser.RequestType requestType) {
      this.requestType = requestType;
      return this;
    }

    public Builder setPathType(MetricsRequestParser.PathType pathType) {
      this.pathType = pathType;
      return this;
    }

    public Builder setRequestId(String requestId) {
      this.requestId = requestId;
      return this;
    }

    public Builder setComponentId(String componentId) {
      this.componentId = componentId;
      return this;
    }

    public Builder setTag(TagType tagType, String tag) {
      this.tagType = tagType;
      this.tag = tag;
      return this;
    }

    public Builder setRunId(String runId) {
      this.runId = runId;
      return this;
    }

    public MetricsRequestContext build() {
      return new MetricsRequestContext(namespaceId, typeId, pathType, requestType, requestId, componentId, tagType, tag,
                                       runId);
    }
  }
}
