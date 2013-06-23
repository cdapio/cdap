/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.program;

/**
 * Defines types of programs supported by the system.
 */
public enum Type {
  FLOW(1),
  PROCEDURE(2),
  MAPREDUCE(3);

  private final int programType;

  private Type(int type) {
    this.programType = type;
  }
}
