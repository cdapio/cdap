/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a flowlet.
 */
public class FlowletId extends NamespacedEntityId implements ParentedId<ProgramId> {
  private final String application;
  private final String version;
  private final String flow;
  private final String flowlet;
  private transient Integer hashCode;

  public FlowletId(String namespace, String application, String flow, String flowlet) {
    this(new ApplicationId(namespace, application), flow, flowlet);
  }

  public FlowletId(ApplicationId appId, String flow, String flowlet) {
    super(appId.getNamespace(), EntityType.FLOWLET);
    if (flow == null) {
      throw new NullPointerException("Flow cannot be null");
    }
    if (flowlet == null) {
      throw new NullPointerException("Flowlet cannot be null");
    }
    this.application = appId.getApplication();
    this.version = appId.getVersion();
    this.flow = flow;
    this.flowlet = flowlet;
  }

  public String getApplication() {
    return application;
  }

  public String getVersion() {
    return version;
  }

  public String getFlow() {
    return flow;
  }

  public String getFlowlet() {
    return flowlet;
  }

  @Override
  public String getEntityName() {
    return getFlowlet();
  }

  @Override
  public ProgramId getParent() {
    return new ProgramId(new ApplicationId(getNamespace(), getApplication(), getVersion()),
                         ProgramType.FLOW, getFlow());
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    FlowletId flowletId = (FlowletId) o;
    return Objects.equals(getParent(), flowletId.getParent()) &&
      Objects.equals(flowlet, flowletId.flowlet);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), getNamespace(), getApplication(), getVersion(),
                                              getFlow(), flowlet);
    }
    return hashCode;
  }

  public FlowletQueueId queue(String queue) {
    // Note: FlowletQueueId is not versioned
    return new FlowletQueueId(getNamespace(), getApplication(), getFlow(), flowlet, queue);
  }

  @SuppressWarnings("unused")
  public static FlowletId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new FlowletId(
      new ApplicationId(next(iterator, "namespace"), next(iterator, "application"), next(iterator, "version")),
      next(iterator, "flow"), nextAndEnd(iterator, "flowlet"));
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.unmodifiableList(Arrays.asList(getNamespace(), getApplication(), getVersion(),
                                                      getFlow(), flowlet));
  }

  @Override
  public Id.Flow.Flowlet toId() {
    return Id.Flow.Flowlet.from(Id.Application.from(getNamespace(), getApplication()), getFlow(), flowlet);
  }

  public static FlowletId fromString(String string) {
    return EntityId.fromString(string, FlowletId.class);
  }
}
