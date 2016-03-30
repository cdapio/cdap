/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.CollapsedRelation;
import co.cask.cdap.data2.metadata.lineage.LineageCollapser;
import co.cask.cdap.data2.metadata.lineage.Relation;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.metadata.lineage.CollapseType;
import com.google.common.collect.ImmutableSet;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

/**
 * Test {@link LineageCollapser}.
 */
public class LineageCollapserTest {
  private final Id.DatasetInstance data1 = Id.DatasetInstance.from("n1", "d1");
  private final Id.DatasetInstance data2 = Id.DatasetInstance.from("n1", "d2");

  private final Id.Flow flow1 = Id.Flow.from("n1", "app1", "flow1");
  private final Id.Flow flow2 = Id.Flow.from("n1", "app2", "flow1");
  
  private final RunId runId1 = new TestRunId("r1");
  private final RunId runId2 = new TestRunId("r2");
  private final RunId runId3 = new TestRunId("r3");

  private final Id.NamespacedId flowlet11 = Id.Flow.Flowlet.from(flow1, "flowlet1");
  private final Id.NamespacedId flowlet12 = Id.Flow.Flowlet.from(flow1, "flowlet2");
  private final Id.NamespacedId flowlet21 = Id.Flow.Flowlet.from(flow2, "flowlet1");
  private final Id.NamespacedId flowlet22 = Id.Flow.Flowlet.from(flow2, "flowlet2");

  @Test
  public void testCollapseAccess() throws Exception {
    Set<Relation> relations = ImmutableSet.of(
      new Relation(data1, flow1, AccessType.READ, runId1, ImmutableSet.of(flowlet11)),
      new Relation(data1, flow1, AccessType.WRITE, runId1, ImmutableSet.of(flowlet11)),
      new Relation(data1, flow1, AccessType.READ, runId1, ImmutableSet.of(flowlet12))
    );

    // Collapse on access
    Assert.assertEquals(
      toSet(
        new CollapsedRelation(data1, flow1, toSet(AccessType.READ, AccessType.WRITE), toSet(runId1), toSet(flowlet11)),
        new CollapsedRelation(data1, flow1, toSet(AccessType.READ), toSet(runId1), toSet(flowlet12))
      ),
      LineageCollapser.collapseRelations(relations, ImmutableSet.of(CollapseType.ACCESS))
    );
  }

  @Test
  public void testCollapseMulti() throws Exception {
    Set<Relation> relations = ImmutableSet.of(
      new Relation(data1, flow1, AccessType.READ, runId1, ImmutableSet.of(flowlet11)),
      new Relation(data1, flow1, AccessType.WRITE, runId1, ImmutableSet.of(flowlet11)),
      new Relation(data1, flow1, AccessType.READ, runId1, ImmutableSet.of(flowlet12)),

      new Relation(data1, flow2, AccessType.READ, runId1, ImmutableSet.of(flowlet11)),
      new Relation(data1, flow2, AccessType.READ, runId1, ImmutableSet.of(flowlet11)),

      new Relation(data2, flow1, AccessType.READ, runId1, ImmutableSet.of(flowlet11)),
      new Relation(data2, flow1, AccessType.READ, runId1, ImmutableSet.of(flowlet11))
    );

    // Collapse on access
    Assert.assertEquals(
      toSet(
        new CollapsedRelation(data1, flow1, toSet(AccessType.READ, AccessType.WRITE), toSet(runId1), toSet(flowlet11)),
        new CollapsedRelation(data1, flow1, toSet(AccessType.READ), toSet(runId1), toSet(flowlet12)),
        new CollapsedRelation(data1, flow2, toSet(AccessType.READ), toSet(runId1), toSet(flowlet11)),
        new CollapsedRelation(data2, flow1, toSet(AccessType.READ), toSet(runId1), toSet(flowlet11))
      ),
      LineageCollapser.collapseRelations(relations, ImmutableSet.of(CollapseType.ACCESS))
    );
  }

  @Test
  public void testCollapseRun() throws Exception {
    Set<Relation> relations = ImmutableSet.of(
      new Relation(data1, flow1, AccessType.READ, runId1, ImmutableSet.of(flowlet11)),
      new Relation(data1, flow1, AccessType.WRITE, runId1, ImmutableSet.of(flowlet11)),
      new Relation(data1, flow1, AccessType.READ, runId2, ImmutableSet.of(flowlet11))
    );

    // Collapse on run
    Assert.assertEquals(
      toSet(
        new CollapsedRelation(data1, flow1, toSet(AccessType.READ), toSet(runId1, runId2), toSet(flowlet11)),
        new CollapsedRelation(data1, flow1, toSet(AccessType.WRITE), toSet(runId1), toSet(flowlet11))
      ),
      LineageCollapser.collapseRelations(relations, ImmutableSet.of(CollapseType.RUN))
    );
  }

  @Test
  public void testCollapseComponent() throws Exception {
    Set<Relation> relations = ImmutableSet.of(
      new Relation(data1, flow1, AccessType.READ, runId1, ImmutableSet.of(flowlet11)),
      new Relation(data1, flow1, AccessType.WRITE, runId1, ImmutableSet.of(flowlet11)),
      new Relation(data1, flow1, AccessType.READ, runId1, ImmutableSet.of(flowlet12))
    );

    // Collapse on component
    Assert.assertEquals(
      toSet(
        new CollapsedRelation(data1, flow1, toSet(AccessType.READ), toSet(runId1), toSet(flowlet11, flowlet12)),
        new CollapsedRelation(data1, flow1, toSet(AccessType.WRITE), toSet(runId1), toSet(flowlet11))
      ),
      LineageCollapser.collapseRelations(relations, ImmutableSet.of(CollapseType.COMPONENT))
    );
  }

  @Test
  public void testCollapseCombinations() throws Exception {
    Set<Relation> relations = ImmutableSet.of(
      // First run
      new Relation(data1, flow1, AccessType.READ, runId1, ImmutableSet.of(flowlet11)),
      new Relation(data1, flow1, AccessType.WRITE, runId1, ImmutableSet.of(flowlet11)),
      new Relation(data1, flow1, AccessType.READ, runId1, ImmutableSet.of(flowlet12)),

      // Second run
      new Relation(data1, flow1, AccessType.READ, runId2, ImmutableSet.of(flowlet11)),
      new Relation(data1, flow1, AccessType.WRITE, runId2, ImmutableSet.of(flowlet11)),
      new Relation(data1, flow1, AccessType.READ, runId2, ImmutableSet.of(flowlet12)),
      new Relation(data1, flow1, AccessType.UNKNOWN, runId2, ImmutableSet.of(flowlet12)),

      // Third run
      new Relation(data1, flow1, AccessType.READ, runId3, ImmutableSet.of(flowlet12)),
      new Relation(data1, flow1, AccessType.UNKNOWN, runId3, ImmutableSet.of(flowlet12))
    );

    // Collapse on access type, run
    Assert.assertEquals(
      toSet(
        new CollapsedRelation(data1, flow1, toSet(AccessType.READ, AccessType.WRITE),
                              toSet(runId1, runId2), toSet(flowlet11)),
        new CollapsedRelation(data1, flow1, toSet(AccessType.READ, AccessType.UNKNOWN),
                              toSet(runId1, runId2, runId3), toSet(flowlet12))
      ),
      LineageCollapser.collapseRelations(relations, toSet(CollapseType.ACCESS, CollapseType.RUN))
    );

    // Collapse on access type, component
    Assert.assertEquals(
      toSet(
        new CollapsedRelation(data1, flow1, toSet(AccessType.READ, AccessType.WRITE),
                              toSet(runId1), toSet(flowlet11, flowlet12)),
        new CollapsedRelation(data1, flow1,
                              toSet(AccessType.READ, AccessType.WRITE, AccessType.UNKNOWN),
                              toSet(runId2), toSet(flowlet11, flowlet12)),
        new CollapsedRelation(data1, flow1, toSet(AccessType.READ, AccessType.UNKNOWN),
                              toSet(runId3), toSet(flowlet12))
      ),
      LineageCollapser.collapseRelations(relations, toSet(CollapseType.ACCESS, CollapseType.COMPONENT))
    );

    // Collapse on component, run
    Assert.assertEquals(
      toSet(
        new CollapsedRelation(data1, flow1, toSet(AccessType.READ), toSet(runId1, runId2, runId3),
                              toSet(flowlet11, flowlet12)),
        new CollapsedRelation(data1, flow1, toSet(AccessType.WRITE), toSet(runId1, runId2), toSet(flowlet11)),
        new CollapsedRelation(data1, flow1, toSet(AccessType.UNKNOWN), toSet(runId2, runId3), toSet(flowlet12))
      ),
      LineageCollapser.collapseRelations(relations, toSet(CollapseType.COMPONENT, CollapseType.RUN))
    );

    // Collapse on all three
    Assert.assertEquals(
      toSet(
        new CollapsedRelation(data1, flow1, toSet(AccessType.READ, AccessType.WRITE, AccessType.UNKNOWN),
                              toSet(runId1, runId2, runId3),
                              toSet(flowlet11, flowlet12))
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

    public TestRunId(String runId) {
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
