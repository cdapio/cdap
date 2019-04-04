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

package io.cdap.cdap.data2.metadata.lineage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

/**
 * Tests storage and retrieval of Dataset accesses by Programs in {@link LineageTable}.
 */
public abstract class LineageTableTest {
  protected static TransactionRunner transactionRunner;

  @Before
  public void before() {
    TransactionRunners.run(transactionRunner, context -> {
      LineageTable lineageTable = LineageTable.create(context);
      lineageTable.deleteAll();
    });
  }

  @Test
  public void testOneRelation() {
    final RunId runId = RunIds.generate(10000);
    final DatasetId datasetInstance = new DatasetId("default", "dataset1");
    final ProgramId program = new ProgramId("default", "app1", ProgramType.SERVICE, "service1");
    final ProgramRunId run = program.run(runId.getId());

    final long accessTimeMillis = System.currentTimeMillis();
    TransactionRunners.run(transactionRunner, context -> {
      LineageTable lineageTable = LineageTable.create(context);
      lineageTable.addAccess(run, datasetInstance, AccessType.READ, accessTimeMillis);

      Relation expected = new Relation(datasetInstance, program, AccessType.READ, runId);
      Set<Relation> relations = lineageTable.getRelations(datasetInstance, 0, 100000, x -> true);
      Assert.assertEquals(1, relations.size());
      Assert.assertEquals(expected, relations.iterator().next());
      Assert.assertEquals(toSet(program, datasetInstance), lineageTable.getEntitiesForRun(run));
      Assert.assertEquals(ImmutableList.of(accessTimeMillis), lineageTable.getAccessTimesForRun(run));
    });
  }

  @Test
  public void testMultipleRelations() {
    final RunId runId1 = RunIds.generate(10000);
    final RunId runId2 = RunIds.generate(20000);
    final RunId runId3 = RunIds.generate(30000);
    final RunId runId4 = RunIds.generate(40000);

    final DatasetId datasetInstance1 = NamespaceId.DEFAULT.dataset("dataset1");
    final DatasetId datasetInstance2 = NamespaceId.DEFAULT.dataset("dataset2");

    final ProgramId program1 = NamespaceId.DEFAULT.app("app1").spark("spark1");
    final ProgramId program2 = NamespaceId.DEFAULT.app("app2").worker("worker2");
    final ProgramId program3 = NamespaceId.DEFAULT.app("app3").service("service3");

    final ProgramRunId run11 = program1.run(runId1.getId());
    final ProgramRunId run22 = program2.run(runId2.getId());
    final ProgramRunId run23 = program2.run(runId3.getId());
    final ProgramRunId run34 = program3.run(runId4.getId());

    final long now = System.currentTimeMillis();
    final long run11Data1AccessTime = now;
    final long run22Data2AccessTime = now + 1;
    final long run23Data2AccessTime = now + 3;
    //noinspection UnnecessaryLocalVariable
    TransactionRunners.run(transactionRunner, context -> {
      LineageTable lineageTable = LineageTable.create(context);
      lineageTable.addAccess(run11, datasetInstance1, AccessType.READ, run11Data1AccessTime);
      lineageTable.addAccess(run22, datasetInstance2, AccessType.WRITE, run22Data2AccessTime);
      lineageTable.addAccess(run23, datasetInstance2, AccessType.WRITE, run23Data2AccessTime);
      lineageTable.addAccess(run34, datasetInstance2, AccessType.READ_WRITE, System.currentTimeMillis());
    });

    TransactionRunners.run(transactionRunner, context -> {
      LineageTable lineageTable = LineageTable.create(context);
      Assert.assertEquals(
        ImmutableSet.of(new Relation(datasetInstance1, program1, AccessType.READ, runId1)),
        lineageTable.getRelations(datasetInstance1, 0, 100000, x -> true)
      );

      Assert.assertEquals(
        ImmutableSet.of(new Relation(datasetInstance2, program2, AccessType.WRITE, runId2),
                        new Relation(datasetInstance2, program2, AccessType.WRITE, runId3),
                        new Relation(datasetInstance2, program3, AccessType.READ_WRITE, runId4)
        ),
        lineageTable.getRelations(datasetInstance2, 0, 100000, x -> true)
      );


      Assert.assertEquals(
        ImmutableSet.of(new Relation(datasetInstance2, program2, AccessType.WRITE, runId2),
                        new Relation(datasetInstance2, program2, AccessType.WRITE, runId3)
        ),
        lineageTable.getRelations(program2, 0, 100000, x -> true)
      );

      // Reduced time range
      Assert.assertEquals(
        ImmutableSet.of(new Relation(datasetInstance2, program2, AccessType.WRITE, runId2),
                        new Relation(datasetInstance2, program2, AccessType.WRITE, runId3)
        ),
        lineageTable.getRelations(datasetInstance2, 0, 35000, x -> true)
      );

      Assert.assertEquals(toSet(program1, datasetInstance1), lineageTable.getEntitiesForRun(run11));
      Assert.assertEquals(ImmutableList.of(run11Data1AccessTime), lineageTable.getAccessTimesForRun(run11));
    });
  }

  @SafeVarargs
  private static <T> Set<T> toSet(T... elements) {
    return ImmutableSet.copyOf(elements);
  }
}
