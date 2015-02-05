/*
 * Copyright © 2014-2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.queue;

import co.cask.cdap.ToyApp;
import co.cask.cdap.WordCountApp;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.queue.QueueSpecification;
import co.cask.cdap.app.queue.QueueSpecificationGenerator;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.Specifications;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.internal.DefaultId;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;
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

  private static final Id.Namespace TEST_NAMESPACE_ID = DefaultId.NAMESPACE;

  private static Table<QueueSpecificationGenerator.Node, String, Set<QueueSpecification>> table
    = HashBasedTable.create();

  private Set<QueueSpecification> get(FlowletConnection.Type sourceType, String sourceName, String target) {
    QueueSpecificationGenerator.Node node = new QueueSpecificationGenerator.Node(sourceType, sourceName);
    return table.get(node, target);
  }

  private boolean containsQueue(Set<QueueSpecification> spec, final String queueName) {
    return Iterables.any(spec, new Predicate<QueueSpecification>() {
      @Override
      public boolean apply(QueueSpecification input) {
        return input.getQueueName().toString().equals(queueName);
      }
    });
  }

  @Before
  public void before() throws Exception {
    table.clear();
  }

  @Test
  public void testQueueSpecificationGenWithToyApp() throws Exception {
    ApplicationSpecification appSpec = Specifications.from(new ToyApp());
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));

    QueueSpecificationGenerator generator =
      new SimpleQueueSpecificationGenerator(Id.Application.from(TEST_NAMESPACE_ID, newSpec.getName()));
    table = generator.create(newSpec.getFlows().values().iterator().next());

    dumpConnectionQueue(table);

    // Stream X
    Assert.assertTrue(containsQueue(get(FlowletConnection.Type.STREAM, "X", "A"), "stream:///X"));
    Assert.assertTrue(containsQueue(get(FlowletConnection.Type.STREAM, "Y", "B"), "stream:///Y"));

    // Node A
    Assert.assertTrue(containsQueue(get(FlowletConnection.Type.FLOWLET, "A", "E"),
                                    String.format("queue:///%s/ToyApp/ToyFlow/A/out1", Constants.DEFAULT_NAMESPACE)));
    Assert.assertTrue(containsQueue(get(FlowletConnection.Type.FLOWLET, "A", "C"),
                                    String.format("queue:///%s/ToyApp/ToyFlow/A/queue", Constants.DEFAULT_NAMESPACE)));

    // Node B
    Assert.assertTrue(containsQueue(get(FlowletConnection.Type.FLOWLET, "B", "E"),
                                    String.format("queue:///%s/ToyApp/ToyFlow/B/queue", Constants.DEFAULT_NAMESPACE)));

    // Node C
    Assert.assertTrue(containsQueue(get(FlowletConnection.Type.FLOWLET, "C", "D"),
                                    String.format("queue:///%s/ToyApp/ToyFlow/C/c1", Constants.DEFAULT_NAMESPACE)));
    Assert.assertTrue(containsQueue(get(FlowletConnection.Type.FLOWLET, "C", "F"),
                                    String.format("queue:///%s/ToyApp/ToyFlow/C/c2", Constants.DEFAULT_NAMESPACE)));

    // Node D
    Assert.assertTrue(containsQueue(get(FlowletConnection.Type.FLOWLET, "D", "G"),
                                    String.format("queue:///%s/ToyApp/ToyFlow/D/d1", Constants.DEFAULT_NAMESPACE)));

    // Node E
    Assert.assertTrue(containsQueue(get(FlowletConnection.Type.FLOWLET, "E", "G"),
                                    String.format("queue:///%s/ToyApp/ToyFlow/E/queue", Constants.DEFAULT_NAMESPACE)));

    // Node F
    Assert.assertTrue(containsQueue(get(FlowletConnection.Type.FLOWLET, "F", "G"),
                                    String.format("queue:///%s/ToyApp/ToyFlow/F/f1", Constants.DEFAULT_NAMESPACE)));
  }

  @Test
  public void testQueueSpecificationGenWithWordCount() throws Exception {
    ApplicationSpecification appSpec = Specifications.from(new WordCountApp());
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));

    QueueSpecificationGenerator generator =
      new SimpleQueueSpecificationGenerator(Id.Application.from(TEST_NAMESPACE_ID, newSpec.getName()));
    table = generator.create(newSpec.getFlows().values().iterator().next());

    Assert.assertEquals(get(FlowletConnection.Type.STREAM, "text", "StreamSource")
                          .iterator().next().getQueueName().toString(), "stream:///text");
    Assert.assertEquals(get(FlowletConnection.Type.FLOWLET, "StreamSource", "Tokenizer")
                        .iterator().next().getQueueName().toString(),
                        String.format("queue:///%s/WordCountApp/WordCountFlow/StreamSource/queue",
                                      Constants.DEFAULT_NAMESPACE));
    Assert.assertEquals(1, get(FlowletConnection.Type.FLOWLET, "Tokenizer", "CountByField").size());
  }

  private void dumpConnectionQueue(Table<QueueSpecificationGenerator.Node, String, Set<QueueSpecification>> table) {
    for (Table.Cell<QueueSpecificationGenerator.Node, String, Set<QueueSpecification>> cell : table.cellSet()) {
      System.out.print(cell.getRowKey().getType() + ":" + cell.getRowKey().getName() + " -> " + cell.getColumnKey() +
                         " = ");

      System.out.println(Joiner.on(" , ").join(Iterables.transform(cell.getValue(),
                                                                   new Function<QueueSpecification, String>() {
        @Override
        public String apply(QueueSpecification input) {
          return input.getQueueName().toString();
        }
      })));
    }
  }
}
