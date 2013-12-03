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
  private final String appId;
  private final String programId;
  private final String componentId;
  private final String contextPrefix;
  private final TagType tagType;
  private final String tag;
  private final MetricsRequestParser.ProgramType programType;

  /**
   * Represents the tag type for metrics context.
   */
  public enum TagType {
    STREAM,
    DATASET,
    QUEUE
  }

  private MetricsRequestContext(String appId, MetricsRequestParser.ProgramType programType,
                                String programId, String componentId, TagType tagType, String tag) {
    this.appId = appId;
    this.programType = programType;
    this.programId = programId;
    this.componentId = componentId;
    this.tagType = tagType;
    this.tag = tag;

    List<String> contextParts = Lists.newArrayListWithCapacity(4);
    if (appId == null || appId.isEmpty()) {
      this.contextPrefix = null;
    } else {
      contextParts.add(appId);
      if (programType != null) {
        contextParts.add(programType.getCode());
        if (programId != null && !programId.isEmpty()) {
          contextParts.add(programId);
          if (componentId != null && !componentId.isEmpty()) {
            contextParts.add(componentId);
          }
        }
      }
      this.contextPrefix = Joiner.on(".").join(contextParts);
    }
  }

  public String getAppId() {
    return appId;
  }

  public String getProgramId() {
    return programId;
  }

  public MetricsRequestParser.ProgramType getProgramType() {
    return programType;
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
    private String appId;
    private String programId;
    private String componentId;
    private TagType tagType;
    private String tag;
    private MetricsRequestParser.ProgramType programType;

    public Builder setAppId(String appId) {
      this.appId = appId;
      return this;
    }

    public Builder setProgramType(MetricsRequestParser.ProgramType programType) {
      this.programType = programType;
      return this;
    }

    public Builder setProgramId(String programId) {
      this.programId = programId;
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
      return new MetricsRequestContext(appId, programType, programId, componentId, tagType, tag);
    }
  }
}
