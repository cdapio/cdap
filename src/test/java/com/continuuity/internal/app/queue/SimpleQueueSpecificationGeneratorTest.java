/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.queue;

import com.continuuity.ToyApp;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.queue.QueueSpecification;
import com.continuuity.app.queue.QueueSpecificationGenerator;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.app.queue.SimpleQueueSpecificationGenerator;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.google.common.collect.Table;
import org.junit.Assert;
import org.junit.Test;

/**
 * The tests here test whether the queue naming working correctly.
 * The <code>ToyApp</code> is to check for connectivity.
 */
public class SimpleQueueSpecificationGeneratorTest {

  @Test
  public void testQueueSpecificationGenWithToyApp() throws Exception {
    ApplicationSpecification appSpec = new ToyApp().configure();
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));

    QueueSpecificationGenerator generator = new SimpleQueueSpecificationGenerator("demo");
    Table<String, String, QueueSpecification> table = generator.create(newSpec.getFlows().values().iterator().next());

    // Stream X
    Assert.assertTrue(table.row("X").get("A").getQueueName().toString().equals("stream://demo/X"));
    Assert.assertTrue(table.row("Y").get("B").getQueueName().toString().equals("stream://demo/Y"));

    // Node A
    Assert.assertTrue(table.row("A").get("E").getQueueName().toString().equals("queue://ToyFlow/A/out1"));
    Assert.assertTrue(table.row("A").get("C").getQueueName().toString().equals("queue://ToyFlow/A/out"));

    // Node B
    Assert.assertTrue(table.row("B").get("E").getQueueName().toString().equals("queue://ToyFlow/B/out"));

    // Node C
    Assert.assertTrue(table.row("C").get("D").getQueueName().toString().equals("queue://ToyFlow/C/c1"));
    Assert.assertTrue(table.row("C").get("F").getQueueName().toString().equals("queue://ToyFlow/C/c2"));

    // Node D
    Assert.assertTrue(table.row("D").get("G").getQueueName().toString().equals("queue://ToyFlow/D/d1"));

    // Node E
    Assert.assertTrue(table.row("E").get("G").getQueueName().toString().equals("queue://ToyFlow/E/out"));

    // Node F
    Assert.assertTrue(table.row("F").get("G").getQueueName().toString().equals("queue://ToyFlow/F/f1"));
  }
}
