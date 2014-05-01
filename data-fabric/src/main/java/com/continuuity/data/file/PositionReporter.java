/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.file;

/**
 * Represents class that carry position information.
 *
 * @param <P> Type of position object.]
 */
public interface PositionReporter<P> {

  /**
   * Returns the position information.
   *
   * @return An instance of type {@code <P>}
   */
  P getPosition();
}
