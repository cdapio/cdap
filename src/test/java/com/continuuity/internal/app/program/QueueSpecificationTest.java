/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.program;

import com.continuuity.ToyApp;
import com.continuuity.WebCrawlApp;
import com.continuuity.WordCountApp;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.program.QueueSpecificationGenerator;
import com.continuuity.app.program.QueueSpecificationHolder;
import com.continuuity.app.program.SimpleQueueSpecificationGenerator;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 *
 */
public class QueueSpecificationTest {

  @Test
  public void testQueueGeneration() throws Exception {
    ApplicationSpecification appSpec = new ToyApp().configure();
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));

    QueueSpecificationGenerator generator = new SimpleQueueSpecificationGenerator();
    Map<String, QueueSpecificationHolder> queues =
      generator.create("demo", newSpec.getName(), newSpec.getFlows().values().iterator().next());
    Assert.assertNotNull(queues);
  }
}
