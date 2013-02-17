/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow;

/**
 * Class that defines a connection between two flowlets.
 */
public final class FlowletConnection {

  /**
   * Defines different types of source a flowlet can be connected to.
   */
  public enum SourceType {
    STREAM,
    FLOWLET
  }

  private final SourceType sourceType;
  private final String sourceName;
  private final String targetName;

  public FlowletConnection(SourceType sourceType, String sourceName, String targetName) {
    this.sourceType = sourceType;
    this.sourceName = sourceName;
    this.targetName = targetName;
  }

  /**
   * @return Type of source.
   */
  public SourceType getSourceType() {
    return sourceType;
  }

  /**
   * @return name of the source.
   */
  public String getSourceName() {
    return sourceName;
  }

  /**
   * @return name of the flowlet, the connection is connected to.
   */
  public String getTargetName() {
    return targetName;
  }

}
