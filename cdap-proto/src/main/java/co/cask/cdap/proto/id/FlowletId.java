/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.proto.id;


import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.element.EntityType;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a flowlet.
 */
public class FlowletId extends EntityId implements NamespacedId, ParentedId<ProgramId> {
  private final String namespace;
  private final String application;
  private final String flow;
  private final String flowlet;

  public FlowletId(String namespace, String application, String flow, String flowlet) {
    super(EntityType.FLOWLET);
    this.namespace = namespace;
    this.application = application;
    this.flow = flow;
    this.flowlet = flowlet;
  }

  public String getApplication() {
    return application;
  }

  public String getFlow() {
    return flow;
  }

  public String getFlowlet() {
    return flowlet;
  }

  @Override
  public ProgramId getParent() {
    return new ProgramId(namespace, application, ProgramType.FLOW, flow);
  }

  @Override
  public String getNamespace() {
    return namespace;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    FlowletId flowletId = (FlowletId) o;
    return Objects.equals(namespace, flowletId.namespace) &&
      Objects.equals(application, flowletId.application) &&
      Objects.equals(flow, flowletId.flow) &&
      Objects.equals(flowlet, flowletId.flowlet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), namespace, application, flow, flowlet);
  }

  public FlowletQueueId queue(String queue) {
    return new FlowletQueueId(namespace, application, flow, flowlet, queue);
  }

  @SuppressWarnings("unused")
  public static FlowletId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new FlowletId(
      next(iterator, "namespace"), next(iterator, "application"),
      next(iterator, "flow"), nextAndEnd(iterator, "flowlet"));
  }

  @Override
  protected Iterable<String> toIdParts() {
    return ImmutableList.of(namespace, application, flow, flowlet);
  }

  @Override
  public Id toId() {
    return Id.Flow.Flowlet.from(Id.Application.from(namespace, application), flow, flowlet);
  }

  public static FlowletId fromString(String string) {
    return EntityId.fromString(string, FlowletId.class);
  }
}
