/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.discovery;

import com.continuuity.weave.discovery.Discoverable;

import java.util.Iterator;
import java.util.Random;

/**
 *
 */
public final class RandomEndpointStrategy implements EndpointStrategy {

  private final Iterable<Discoverable> endpoints;

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
