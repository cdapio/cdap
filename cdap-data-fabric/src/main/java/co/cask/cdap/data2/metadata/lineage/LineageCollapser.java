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

package co.cask.cdap.data2.metadata.lineage;

import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.metadata.lineage.CollapseType;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Collapses {@link Relation Relations} based on {@link CollapseType}
 */
public final class LineageCollapser {
  private LineageCollapser() {
    // cannot instantiate objects
  }

  private static final Logger LOG = LoggerFactory.getLogger(LineageCollapser.class);

  /**
   * Collapse {@link Relation}s based on {@link CollapseType}
   * @param relations lineage relations
   * @param collapseTypes fields to collapse relations on
   * @return collapsed relations
   */
  public static Set<CollapsedRelation> collapseRelations(Iterable<Relation> relations,
                                                         Set<CollapseType> collapseTypes) {
    Set<CollapsedRelation> collapsedRelations = new HashSet<>();

    Multimap<CollapseKey, Relation> multimap = HashMultimap.create();
    for (Relation relation : relations) {
      multimap.put(getCollapseKey(relation, collapseTypes), relation);
    }
    LOG.trace("Collapsed relations: {}", multimap.asMap());

    for (Map.Entry<CollapseKey, Collection<Relation>> collapsedEntry : multimap.asMap().entrySet()) {
      NamespacedEntityId data = collapsedEntry.getKey().data;
      ProgramId program = collapsedEntry.getKey().program;

      Set<AccessType> accessTypes = new HashSet<>();
      Set<RunId> runs = new HashSet<>();
      Set<NamespacedEntityId> components = new HashSet<>();

      for (Relation relation : collapsedEntry.getValue()) {
        accessTypes.add(relation.getAccess());
        runs.add(relation.getRun());
        components.addAll(relation.getComponents());
      }
      collapsedRelations.add(toCollapsedRelation(data, program, accessTypes, runs, components));
    }
    return collapsedRelations;
  }

  private static CollapseKey getCollapseKey(Relation relation, Set<CollapseType> collapseTypes) {
    CollapseKeyBuilder builder = new CollapseKeyBuilder(relation.getData(), relation.getProgram());
    if (!collapseTypes.contains(CollapseType.ACCESS)) {
      builder.setAccess(relation.getAccess());
    }
    if (!collapseTypes.contains(CollapseType.RUN)) {
      builder.setRun(relation.getRun());
    }
    if (!collapseTypes.contains(CollapseType.COMPONENT)) {
      builder.setComponents(relation.getComponents());
    }
    return builder.build();
  }

  private static final class CollapseKeyBuilder {
    private final NamespacedEntityId data;
    private final ProgramId program;
    private AccessType access;
    private RunId run;
    private Set<NamespacedEntityId> components;

    CollapseKeyBuilder(NamespacedEntityId data, ProgramId program) {
      this.data = data;
      this.program = program;
    }

    public void setAccess(AccessType access) {
      this.access = access;
    }

    public void setRun(RunId run) {
      this.run = run;
    }

    public void setComponents(Set<NamespacedEntityId> components) {
      this.components = components;
    }

    public CollapseKey build() {
      return new CollapseKey(data, program, access, run, components);
    }
  }

  private static CollapsedRelation toCollapsedRelation(NamespacedEntityId data, ProgramId program,
                                                       Set<AccessType> accesses, Set<RunId> runs,
                                                       Set<NamespacedEntityId> components) {
    Preconditions.checkState(data instanceof DatasetId || data instanceof StreamId,
                             "%s should be an instance of dataset or stream", data);
    if (data instanceof DatasetId) {
      return new CollapsedRelation((DatasetId) data, program, accesses, runs, components);
    }
    return new CollapsedRelation((StreamId) data, program, accesses, runs, components);
  }

  private static final class CollapseKey {
    private final NamespacedEntityId data;
    private final ProgramId program;
    private final AccessType access;
    private final RunId run;
    private final Set<? extends NamespacedEntityId> components;

    CollapseKey(NamespacedEntityId data, ProgramId program, AccessType access, RunId run,
                Set<? extends NamespacedEntityId> components) {
      this.data = data;
      this.program = program;
      this.access = access;
      this.run = run;
      this.components = components;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CollapseKey that = (CollapseKey) o;
      return Objects.equals(data, that.data) &&
        Objects.equals(program, that.program) &&
        Objects.equals(access, that.access) &&
        Objects.equals(run, that.run) &&
        Objects.equals(components, that.components);
    }

    @Override
    public int hashCode() {
      return Objects.hash(data, program, access, run, components);
    }

    @Override
    public String toString() {
      return "CollapseKey{" +
        "data=" + data +
        ", program=" + program +
        ", access=" + access +
        ", run=" + run +
        ", components=" + components +
        '}';
    }
  }
}
