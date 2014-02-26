/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.discovery;

import org.apache.twill.discovery.Discoverable;

import java.util.Iterator;
import java.util.Random;

/**
 * Randomly picks endpoint from the list of available endpoints.
 */
public final class RandomEndpointStrategy implements EndpointStrategy {

  private final Iterable<Discoverable> endpoints;

  /**
   * Constructs a random endpoint strategy.
   * @param endpoints Endpoints for the strategy to use. Note that this strategy will
   *                  invoke {@link Iterable#iterator()} and traverse through it on
   *                  every call to the {@link #pick()} method. One could leverage this
   *                  behavior with the live {@link Iterable} as provided by
   *                  {@link org.apache.twill.discovery.DiscoveryServiceClient#discover(String)} method.
   */
  public RandomEndpointStrategy(Iterable<Discoverable> endpoints) {
    this.endpoints = endpoints;
  }

  @Override
  public Discoverable pick() {
    // Reservoir sampling
    Discoverable result = null;
    Iterator<Discoverable> itor = endpoints.iterator();
    Random random = new Random();
    int count = 0;
    while (itor.hasNext()) {
      Discoverable next = itor.next();
      if (random.nextInt(++count) == 0) {
        result = next;
      }
    }
    return result;
  }
}
