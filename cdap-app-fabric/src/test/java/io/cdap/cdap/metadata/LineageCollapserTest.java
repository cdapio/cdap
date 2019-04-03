/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.metadata;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.data2.metadata.lineage.AccessType;
import io.cdap.cdap.data2.metadata.lineage.CollapsedRelation;
import io.cdap.cdap.data2.metadata.lineage.LineageCollapser;
import io.cdap.cdap.data2.metadata.lineage.Relation;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.metadata.lineage.CollapseType;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

/**
 * Test {@link LineageCollapser}.
 */
public class LineageCollapserTest {
  private final DatasetId data1 = new DatasetId("n1", "d1");
  private final DatasetId data2 = new DatasetId("n1", "d2");

  private final ProgramId service1 = new ProgramId("n1", "app1", ProgramType.SERVICE, "ser1");
  private final ProgramId service2 = new ProgramId("n1", "app2", ProgramType.SERVICE, "ser2");
  
  private final RunId runId1 = new TestRunId("r1");
  private final RunId runId2 = new TestRunId("r2");
  private final RunId runId3 = new TestRunId("r3");

  @Test
  public void testCollapseAccess() {
    Set<Relation> relations = ImmutableSet.of(
      new Relation(data1, service1, AccessType.READ, runId1),
      new Relation(data1, service1, AccessType.WRITE, runId1),
      new Relation(data1, service1, AccessType.READ, runId1)
    );

    // Collapse on access
    Assert.assertEquals(
      toSet(
        new CollapsedRelation(data1, service1, toSet(AccessType.READ, AccessType.WRITE),
                              toSet(runId1), Collections.emptySet())
      ),
      LineageCollapser.collapseRelations(relations, ImmutableSet.of(CollapseType.ACCESS))
    );
  }

  @Test
  public void testCollapseMulti() {
    Set<Relation> relations = ImmutableSet.of(
      new Relation(data1, service1, AccessType.READ, runId1),
      new Relation(data1, service1, AccessType.WRITE, runId1),
      new Relation(data1, service1, AccessType.READ, runId1),

      new Relation(data1, service2, AccessType.READ, runId1),
      new Relation(data1, service2, AccessType.READ, runId1),

      new Relation(data2, service1, AccessType.READ, runId1),
      new Relation(data2, service1, AccessType.READ, runId1)
    );

    // Collapse on access
    Assert.assertEquals(
      toSet(
        new CollapsedRelation(data1, service1, toSet(AccessType.READ, AccessType.WRITE),
                              toSet(runId1), Collections.emptySet()),
        new CollapsedRelation(data1, service2, toSet(AccessType.READ), toSet(runId1), Collections.emptySet()),
        new CollapsedRelation(data2, service1, toSet(AccessType.READ), toSet(runId1), Collections.emptySet())
      ),
      LineageCollapser.collapseRelations(relations, ImmutableSet.of(CollapseType.ACCESS))
    );
  }

  @Test
  public void testCollapseRun() {
    Set<Relation> relations = ImmutableSet.of(
      new Relation(data1, service1, AccessType.READ, runId1),
      new Relation(data1, service1, AccessType.WRITE, runId1),
      new Relation(data1, service1, AccessType.READ, runId2)
    );

    // Collapse on run
    Assert.assertEquals(
      toSet(
        new CollapsedRelation(data1, service1, toSet(AccessType.READ), toSet(runId1, runId2), Collections.emptySet()),
        new CollapsedRelation(data1, service1, toSet(AccessType.WRITE), toSet(runId1), Collections.emptySet())
      ),
      LineageCollapser.collapseRelations(relations, ImmutableSet.of(CollapseType.RUN))
    );
  }

  @Test
  public void testCollapseComponent() {
    Set<Relation> relations = ImmutableSet.of(
      new Relation(data1, service1, AccessType.READ, runId1),
      new Relation(data1, service1, AccessType.WRITE, runId1),
      new Relation(data1, service1, AccessType.READ, runId1)
    );

    // Collapse on component
    Assert.assertEquals(
      toSet(
        new CollapsedRelation(data1, service1, toSet(AccessType.READ), toSet(runId1), Collections.emptySet()),
        new CollapsedRelation(data1, service1, toSet(AccessType.WRITE), toSet(runId1), Collections.emptySet())
      ),
      LineageCollapser.collapseRelations(relations, ImmutableSet.of(CollapseType.COMPONENT))
    );
  }

  @Test
  public void testCollapseCombinations() {
    Set<Relation> relations = ImmutableSet.of(
      // First run
      new Relation(data1, service1, AccessType.READ, runId1),
      new Relation(data1, service1, AccessType.WRITE, runId1),
      new Relation(data1, service1, AccessType.READ, runId1),

      // Second run
      new Relation(data1, service1, AccessType.READ, runId2),
      new Relation(data1, service1, AccessType.WRITE, runId2),
      new Relation(data1, service1, AccessType.READ, runId2),
      new Relation(data1, service1, AccessType.UNKNOWN, runId2),

      // Third run
      new Relation(data1, service1, AccessType.READ, runId3),
      new Relation(data1, service1, AccessType.UNKNOWN, runId3)
    );

    // Collapse on access type, run
    Assert.assertEquals(
      toSet(
        new CollapsedRelation(data1, service1, toSet(AccessType.READ, AccessType.WRITE, AccessType.UNKNOWN),
                              toSet(runId1, runId2, runId3), Collections.emptySet())
      ),
      LineageCollapser.collapseRelations(relations, toSet(CollapseType.ACCESS, CollapseType.RUN))
    );

    // Collapse on access type, component
    Assert.assertEquals(
      toSet(
        new CollapsedRelation(data1, service1, toSet(AccessType.READ, AccessType.WRITE),
                              toSet(runId1), Collections.emptySet()),
        new CollapsedRelation(data1, service1,
                              toSet(AccessType.READ, AccessType.WRITE, AccessType.UNKNOWN),
                              toSet(runId2), Collections.emptySet()),
        new CollapsedRelation(data1, service1, toSet(AccessType.READ, AccessType.UNKNOWN),
                              toSet(runId3), Collections.emptySet())
      ),
      LineageCollapser.collapseRelations(relations, toSet(CollapseType.ACCESS, CollapseType.COMPONENT))
    );

    // Collapse on component, run
    Assert.assertEquals(
      toSet(
        new CollapsedRelation(data1, service1, toSet(AccessType.READ), toSet(runId1, runId2, runId3),
                              Collections.emptySet()),
        new CollapsedRelation(data1, service1, toSet(AccessType.WRITE), toSet(runId1, runId2), Collections.emptySet()),
        new CollapsedRelation(data1, service1, toSet(AccessType.UNKNOWN), toSet(runId2, runId3), Collections.emptySet())
      ),
      LineageCollapser.collapseRelations(relations, toSet(CollapseType.COMPONENT, CollapseType.RUN))
    );

    // Collapse on all three
    Assert.assertEquals(
      toSet(
        new CollapsedRelation(data1, service1, toSet(AccessType.READ, AccessType.WRITE, AccessType.UNKNOWN),
                              toSet(runId1, runId2, runId3),
                              Collections.emptySet())
      ),
      LineageCollapser.collapseRelations(relations,
                                         toSet(CollapseType.COMPONENT, CollapseType.RUN, CollapseType.ACCESS))
    );
  }

  @SafeVarargs
  private static <T> Set<T> toSet(T... elements) {
    return ImmutableSet.<T>builder().add(elements).build();
  }

  private static final class TestRunId implements RunId {
    private final String runId;

    TestRunId(String runId) {
      this.runId = runId;
    }

    @Override
    public String getId() {
      return runId;
    }

    @Override
    public String toString() {
      return runId;
    }
  }
}
