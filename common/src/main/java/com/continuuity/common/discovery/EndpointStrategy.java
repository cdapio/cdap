/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.discovery;

import org.apache.twill.discovery.Discoverable;

/**
 * This class helps picking up an endpoint from a list of Discoverable.
 */
public interface EndpointStrategy {

  /**
   * Picks a {@link Discoverable} using its strategy.
   * @return A {@link Discoverable} based on the stragegy or {@code null} if no endpoint can be found.
   */
  Discoverable pick();
}
