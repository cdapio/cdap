/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow;

/**
 * Defines a connection between two {@link com.continuuity.api.flow.flowlet.Flowlet Flowlets} or
 * from a {@link com.continuuity.api.data.stream.Stream Stream} to a
 * {@link com.continuuity.api.flow.flowlet.Flowlet Flowlet}.
 */
public final class FlowletConnection {

  /**
   * Defines different types of sources a flowlet can be connected to.
   */
  public enum Type {
    STREAM,
    FLOWLET
  }

  private final Type sourceType;
  private final String sourceName;
  private final String targetName;

  public FlowletConnection(Type sourceType, String sourceName, String targetName) {
    this.sourceType = sourceType;
    this.sourceName = sourceName;
    this.targetName = targetName;
  }

  /**
   * @return Type of source.
   */
  public Type getSourceType() {
    return sourceType;
  }

  /**
   * @return Name of the source.
   */
  public String getSourceName() {
    return sourceName;
  }

  /**
   * @return Name of the flowlet the connection is connected to.
   */
  public String getTargetName() {
    return targetName;
  }
}
