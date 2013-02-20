/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.queue;

import com.continuuity.ToyApp;
import com.continuuity.WordCountApp;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.flow.FlowletConnection;
import com.continuuity.app.Id;
import com.continuuity.app.queue.QueueSpecification;
import com.continuuity.app.queue.QueueSpecificationGenerator;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

/**
 * The tests here test whether the queue naming working correctly.
 * The <code>ToyApp</code> is to check for connectivity.
 */
public class SimpleQueueSpecificationGeneratorTest {
  private static Table<QueueSpecificationGenerator.Node, String, Set<QueueSpecification>> table
    = HashBasedTable.create();

  private Set<QueueSpecification> get(FlowletConnection.Type sourceType, String sourceName, String target) {
    QueueSpecificationGenerator.Node node = new QueueSpecificationGenerator.Node(sourceType, sourceName);
    return table.get(node, target);
  }

  @Before
  public void before() throws Exception {
    table.clear();
  }

  @Test
  public void testQueueSpecificationGenWithToyApp() throws Exception {
    ApplicationSpecification appSpec = new ToyApp().configure();
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));

    QueueSpecificationGenerator generator = new SimpleQueueSpecificationGenerator(Id.Account.DEFAULT());
    table = generator.create(newSpec.getFlows().values().iterator().next());

    // Stream X
    Assert.assertTrue(get(FlowletConnection.Type.STREAM, "X", "A")
                        .iterator().next().getQueueName().toString().equals("stream://demo/X"));

    Assert.assertTrue(get(FlowletConnection.Type.STREAM, "Y", "B")
                        .iterator().next().getQueueName().toString().equals("stream://demo/Y"));

    // Node A
    Assert.assertTrue(get(FlowletConnection.Type.FLOWLET, "A", "E")
                        .iterator().next().getQueueName().toString().equals("queue://ToyFlow/A/out1"));
    Assert.assertTrue(get(FlowletConnection.Type.FLOWLET, "A", "C")
                        .iterator().next().getQueueName().toString().equals("queue://ToyFlow/A/out"));

    // Node B
    Assert.assertTrue(get(FlowletConnection.Type.FLOWLET, "B", "E")
                        .iterator().next().getQueueName().toString().equals("queue://ToyFlow/B/out"));

    // Node C
    Assert.assertTrue(get(FlowletConnection.Type.FLOWLET, "C", "D")
                        .iterator().next().getQueueName().toString().equals("queue://ToyFlow/C/c1"));
    Assert.assertTrue(get(FlowletConnection.Type.FLOWLET, "C", "F")
                        .iterator().next().getQueueName().toString().equals("queue://ToyFlow/C/c2"));

    // Node D
    Assert.assertTrue(get(FlowletConnection.Type.FLOWLET, "D", "G")
                        .iterator().next().getQueueName().toString().equals("queue://ToyFlow/D/d1"));

    // Node E
    Assert.assertTrue(get(FlowletConnection.Type.FLOWLET, "E", "G")
                        .iterator().next().getQueueName().toString().equals("queue://ToyFlow/E/out"));

    // Node F
    Assert.assertTrue(get(FlowletConnection.Type.FLOWLET, "F", "G")
                        .iterator().next().getQueueName().toString().equals("queue://ToyFlow/F/f1"));
  }

  @Test
  public void testQueueSpecificationGenWithWordCount() throws Exception {
    ApplicationSpecification appSpec = new WordCountApp().configure();
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));

    QueueSpecificationGenerator generator = new SimpleQueueSpecificationGenerator(Id.Account.DEFAULT());
    table = generator.create(newSpec.getFlows().values().iterator().next());

    Assert.assertTrue(get(FlowletConnection.Type.STREAM, "text", "StreamSucker")
                        .iterator().next().getQueueName().toString().equals("stream://demo/text"));
    Assert.assertTrue(get(FlowletConnection.Type.FLOWLET, "StreamSucker", "Tokenizer")
                        .iterator().next().getQueueName().toString().equals("queue://WordCountFlow/StreamSucker/out"));
    Assert.assertEquals(2, get(FlowletConnection.Type.FLOWLET, "Tokenizer", "CountByField").size());
  }
}