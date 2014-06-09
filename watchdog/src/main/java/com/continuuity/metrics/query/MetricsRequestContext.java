/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Class representing the context for a metrics request, including whether or not there was a tag associated
 * with the request.
 */
public class MetricsRequestContext {
  private final String typeId;
  private final String requestId;
  private final String componentId;
  private final String contextPrefix;
  private final TagType tagType;
  private final String tag;
  private final MetricsRequestParser.RequestType requestType;
  private final MetricsRequestParser.PathType pathType;

  /**
   * Represents the tag type for metrics context.
   */
  public enum TagType {
    STREAM,
    DATASET,
    SERVICE,
    QUEUE
  }

  private MetricsRequestContext(String typeId, MetricsRequestParser.PathType pathType,
                                MetricsRequestParser.RequestType requestType,
                                String requestId, String componentId, TagType tagType, String tag) {
    this.typeId = typeId;
    this.pathType = pathType;
    this.requestType = requestType;
    this.requestId = requestId;
    this.componentId = componentId;
    this.tagType = tagType;
    this.tag = tag;

    List<String> contextParts = Lists.newArrayListWithCapacity(4);
    if (typeId == null || typeId.isEmpty()) {
      this.contextPrefix = null;
    } else {
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

  /**
   * Builds a metrics context.
   */
  public static class Builder {
    private String typeId;
    private String requestId;
    private String componentId;
    private TagType tagType;
    private String tag;
    private MetricsRequestParser.RequestType requestType;
    private MetricsRequestParser.PathType pathType;

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

    public MetricsRequestContext build() {
      return new MetricsRequestContext(typeId, pathType, requestType, requestId, componentId, tagType, tag);
    }
  }
}
