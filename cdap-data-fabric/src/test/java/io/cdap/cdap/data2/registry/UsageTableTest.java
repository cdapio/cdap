/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.registry;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public abstract class UsageTableTest {
  protected static TransactionRunner transactionRunner;

  protected final NamespaceId namespace1 = new NamespaceId("ns1");
  protected final NamespaceId namespace2 = new NamespaceId("ns2");

  protected final ApplicationId app1 = namespace1.app("app1");
  protected final ProgramId worker1 = app1.worker("worker1");
  protected final ProgramId worker2 = app1.worker("worker2");
  protected final ProgramId service11 = app1.service("service11");

  protected final ApplicationId app2 = namespace1.app("app2");
  protected final ProgramId worker21 = app2.worker("worker21");
  protected final ProgramId worker22 = app2.worker("worker22");
  protected final ProgramId service21 = app2.service("service21");

  protected final DatasetId datasetInstance1 = namespace1.dataset("ds1");
  protected final DatasetId datasetInstance2 = namespace1.dataset("ds2");
  protected final DatasetId datasetInstance3 = namespace1.dataset("ds3");

  @Before
  public void before() {
    TransactionRunners.run(transactionRunner, context -> {
      UsageTable usageTable = new UsageTable(context);
      usageTable.deleteAll();
    });
  }

  @Test
  public void testOneMapping() throws Exception {
    // Add mapping
    TransactionRunners.run(transactionRunner, context -> {
      UsageTable usageTable = new UsageTable(context);
      usageTable.register(worker1, datasetInstance1);
    });

    // Verify mapping
    TransactionRunners.run(transactionRunner, context -> {
      UsageTable usageTable = new UsageTable(context);
      Assert.assertEquals(ImmutableSet.of(datasetInstance1), usageTable.getDatasets(worker1));
    });
  }

  @Test
  public void testProgramDatasetMapping() throws Exception {
    // Add mappings
    TransactionRunners.run(transactionRunner, context -> {
      UsageTable usageTable = new UsageTable(context);
      usageTable.register(worker1, datasetInstance1);
      usageTable.register(worker1, datasetInstance3);
      usageTable.register(worker2, datasetInstance2);
      usageTable.register(service11, datasetInstance1);

      usageTable.register(worker21, datasetInstance2);
      usageTable.register(service21, datasetInstance1);
    });

    TransactionRunners.run(transactionRunner, context -> {
      UsageTable usageTable = new UsageTable(context);
      // Verify program mappings
      Assert.assertEquals(ImmutableSet.of(datasetInstance1, datasetInstance3), usageTable.getDatasets(worker1));
      Assert.assertEquals(ImmutableSet.of(datasetInstance2), usageTable.getDatasets(worker2));
      Assert.assertEquals(ImmutableSet.of(datasetInstance1), usageTable.getDatasets(service11));

      Assert.assertEquals(ImmutableSet.of(datasetInstance2), usageTable.getDatasets(worker21));
      Assert.assertEquals(ImmutableSet.of(datasetInstance1), usageTable.getDatasets(service21));

      // Verify app mappings
      Assert.assertEquals(ImmutableSet.of(datasetInstance1, datasetInstance2, datasetInstance3),
                          usageTable.getDatasets(worker1.getParent()));
      Assert.assertEquals(ImmutableSet.of(datasetInstance1, datasetInstance2),
                          usageTable.getDatasets(worker21.getParent()));

      // Verify dataset mappings
      Assert.assertEquals(ImmutableSet.of(worker1, service11, service21), usageTable.getPrograms(datasetInstance1));
      Assert.assertEquals(ImmutableSet.of(worker2, worker21), usageTable.getPrograms(datasetInstance2));
      Assert.assertEquals(ImmutableSet.of(worker1), usageTable.getPrograms(datasetInstance3));
    });

    // --------- Delete app1 -----------
    TransactionRunners.run(transactionRunner, context -> {
      UsageTable usageTable = new UsageTable(context);
      usageTable.unregister(worker1.getParent());
    });

    TransactionRunners.run(transactionRunner, context -> {
      UsageTable usageTable = new UsageTable(context);
      // There should be no mappings for programs of app1 now
      Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageTable.getDatasets(worker1));
      Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageTable.getDatasets(worker2));
      Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageTable.getDatasets(service11));

      Assert.assertEquals(ImmutableSet.of(datasetInstance2), usageTable.getDatasets(worker21));
      Assert.assertEquals(ImmutableSet.of(datasetInstance1), usageTable.getDatasets(service21));

      // Verify app mappings
      Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageTable.getDatasets(worker1.getParent()));
      Assert.assertEquals(ImmutableSet.of(datasetInstance1, datasetInstance2),
                          usageTable.getDatasets(worker21.getParent()));

      // Verify dataset mappings
      Assert.assertEquals(ImmutableSet.of(service21), usageTable.getPrograms(datasetInstance1));
      Assert.assertEquals(ImmutableSet.of(worker21), usageTable.getPrograms(datasetInstance2));
      Assert.assertEquals(ImmutableSet.<ProgramId>of(), usageTable.getPrograms(datasetInstance3));
    });
  }

  @Test
  public void testAllMappings() throws Exception {
    // Add mappings
    TransactionRunners.run(transactionRunner, context -> {
      UsageTable usageTable = new UsageTable(context);
      usageTable.register(worker1, datasetInstance1);
      usageTable.register(service21, datasetInstance3);
    });

    TransactionRunners.run(transactionRunner, context -> {
      UsageTable usageTable = new UsageTable(context);
      // Verify app mappings
      Assert.assertEquals(ImmutableSet.of(datasetInstance1), usageTable.getDatasets(worker1));
      Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageTable.getDatasets(worker2));
      Assert.assertEquals(ImmutableSet.of(datasetInstance3), usageTable.getDatasets(service21));

      // Verify dataset mappings
      Assert.assertEquals(ImmutableSet.of(worker1), usageTable.getPrograms(datasetInstance1));
    });

    // --------- Delete app1 -----------
    TransactionRunners.run(transactionRunner, context -> {
      UsageTable usageTable = new UsageTable(context);
      usageTable.unregister(worker1.getParent());
    });

    TransactionRunners.run(transactionRunner, context -> {
      UsageTable usageTable = new UsageTable(context);
      // Verify app mappings
      Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageTable.getDatasets(worker1));
      Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageTable.getDatasets(worker2));
      Assert.assertEquals(ImmutableSet.of(datasetInstance3), usageTable.getDatasets(service21));

      // Verify dataset mappings
      Assert.assertEquals(ImmutableSet.<ProgramId>of(), usageTable.getPrograms(datasetInstance1));

      // Verify app mappings
      Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageTable.getDatasets(worker1));
      Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageTable.getDatasets(worker2));
      Assert.assertEquals(ImmutableSet.of(datasetInstance3), usageTable.getDatasets(service21));

      // Verify dataset mappings
      Assert.assertEquals(ImmutableSet.<ProgramId>of(), usageTable.getPrograms(datasetInstance1));

      // Verify app mappings
      Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageTable.getDatasets(worker1));
      Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageTable.getDatasets(worker2));
      Assert.assertEquals(ImmutableSet.of(datasetInstance3), usageTable.getDatasets(service21));

      // Verify dataset mappings
      Assert.assertEquals(ImmutableSet.<ProgramId>of(), usageTable.getPrograms(datasetInstance1));
    });

    // --------- Delete app2 -----------
    TransactionRunners.run(transactionRunner, context -> {
      UsageTable usageTable = new UsageTable(context);
      usageTable.unregister(worker21.getParent());
    });

    TransactionRunners.run(transactionRunner, context -> {
      UsageTable usageTable = new UsageTable(context);
      // Verify app mappings
      Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageTable.getDatasets(worker1));
      Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageTable.getDatasets(worker2));
      Assert.assertEquals(ImmutableSet.<DatasetId>of(), usageTable.getDatasets(service21));

      // Verify dataset mappings
      Assert.assertEquals(ImmutableSet.<ProgramId>of(), usageTable.getPrograms(datasetInstance1));
    });
  }
}
