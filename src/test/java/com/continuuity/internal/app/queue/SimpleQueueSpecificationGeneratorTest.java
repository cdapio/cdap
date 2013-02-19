/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.queue;

import com.continuuity.ToyApp;
import com.continuuity.WebCrawlApp;
import com.continuuity.WordCountApp;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.program.Id;
import com.continuuity.app.queue.QueueSpecification;
import com.continuuity.app.queue.QueueSpecificationGenerator;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.app.queue.SimpleQueueSpecificationGenerator;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.google.common.collect.Table;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

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

    QueueSpecificationGenerator generator = new SimpleQueueSpecificationGenerator(Id.Account.DEFAULT());
    Table<String, String, Set<QueueSpecification>> table
      = generator.create(newSpec.getFlows().values().iterator().next());

    // Stream X
    Assert.assertTrue(table.row("X").get("A")
                        .iterator().next().getQueueName().toString().equals("stream://demo/X"));
    Assert.assertTrue(table.row("Y").get("B")
                        .iterator().next().getQueueName().toString().equals("stream://demo/Y"));

    // Node A
    Assert.assertTrue(table.row("A").get("E")
                        .iterator().next().getQueueName().toString().equals("queue://ToyFlow/A/out1"));
    Assert.assertTrue(table.row("A").get("C")
                        .iterator().next().getQueueName().toString().equals("queue://ToyFlow/A/out"));

    // Node B
    Assert.assertTrue(table.row("B").get("E")
                        .iterator().next().getQueueName().toString().equals("queue://ToyFlow/B/out"));

    // Node C
    Assert.assertTrue(table.row("C").get("D")
                        .iterator().next().getQueueName().toString().equals("queue://ToyFlow/C/c1"));
    Assert.assertTrue(table.row("C").get("F")
                        .iterator().next().getQueueName().toString().equals("queue://ToyFlow/C/c2"));

    // Node D
    Assert.assertTrue(table.row("D").get("G")
                        .iterator().next().getQueueName().toString().equals("queue://ToyFlow/D/d1"));

    // Node E
    Assert.assertTrue(table.row("E").get("G")
                        .iterator().next().getQueueName().toString().equals("queue://ToyFlow/E/out"));

    // Node F
    Assert.assertTrue(table.row("F").get("G")
                        .iterator().next().getQueueName().toString().equals("queue://ToyFlow/F/f1"));
  }

  @Test
  public void testQueueSpecificationGenWithWordCount() throws Exception {
    ApplicationSpecification appSpec = new WordCountApp().configure();
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));

    QueueSpecificationGenerator generator = new SimpleQueueSpecificationGenerator(Id.Account.DEFAULT());
    Table<String, String, Set<QueueSpecification>> table
      = generator.create(newSpec.getFlows().values().iterator().next());

    Assert.assertTrue(table.row("text").get("StreamSucker")
                        .iterator().next().getQueueName().toString().equals("stream://demo/text"));
    Assert.assertTrue(table.row("StreamSucker").get("Tokenizer")
                        .iterator().next().getQueueName().toString().equals("queue://WordCountFlow/StreamSucker/out"));
    Assert.assertEquals(table.row("Tokenizer").get("CountByField").size(), 2);
  }
}
