/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class TimeseriesIdTest {

  @Test
  public void testEquality() {
    TimeseriesId id1 = new TimeseriesId("app.f.flow.flowlet.0", "process.events", null, "0");
    TimeseriesId id2 = new TimeseriesId("app.f.flow.flowlet.0", "process.events", null, "0");
    Assert.assertTrue(id1.equals(id2));
    Assert.assertTrue(id2.equals(id1));
    Assert.assertEquals(id1.hashCode(), id2.hashCode());

    id1 = new TimeseriesId("app.f.flow.flowlet.0", "process.events", "tag1", "0");
    id2 = new TimeseriesId("app.f.flow.flowlet.0", "process.events", "tag1", "0");
    Assert.assertTrue(id1.equals(id2));
    Assert.assertTrue(id2.equals(id1));
    Assert.assertEquals(id1.hashCode(), id2.hashCode());
  }
}
