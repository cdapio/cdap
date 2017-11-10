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
import co.cask.cdap.proto.element.EntityType;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a flowlet queue.
 */
public class FlowletQueueId extends NamespacedEntityId implements ParentedId<FlowletId> {
  private final String application;
  private final String flow;
  private final String flowlet;
  private final String queue;
  private transient Integer hashCode;

  public FlowletQueueId(String namespace, String application, String flow, String flowlet, String queue) {
    super(namespace, EntityType.FLOWLET_QUEUE);
    if (application == null) {
      throw new NullPointerException("Application cannot be null");
    }
    if (flow == null) {
      throw new NullPointerException("Flow cannot be null");
    }
    if (flowlet == null) {
      throw new NullPointerException("Flowlet cannot be null");
    }
    if (queue == null) {
      throw new NullPointerException("Queue cannot be null");
    }
    this.application = application;
    this.flow = flow;
    this.flowlet = flowlet;
    this.queue = queue;
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

  public String getQueue() {
    return queue;
  }

  @Override
  public String getEntityName() {
    return getQueue();
  }

  @Override
  public FlowletId getParent() {
    return new FlowletId(namespace, application, flow, flowlet);
  }

  @SuppressWarnings("unused")
  public static FlowletQueueId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new FlowletQueueId(
      next(iterator, "namespace"), next(iterator, "application"),
      next(iterator, "flow"), next(iterator, "flowlet"),
      nextAndEnd(iterator, "queue"));
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    FlowletQueueId that = (FlowletQueueId) o;
    return Objects.equals(namespace, that.namespace) &&
      Objects.equals(application, that.application) &&
      Objects.equals(flow, that.flow) &&
      Objects.equals(flowlet, that.flowlet) &&
      Objects.equals(queue, that.queue);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), namespace, application, flow, flowlet, queue);
    }
    return hashCode;
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.unmodifiableList(Arrays.asList(namespace, application, flow, flowlet, queue));
  }

  @Override
  public Id.Flow.Flowlet.Queue toId() {
    return new Id.Flow.Flowlet.Queue(
      Id.Flow.Flowlet.from(Id.Application.from(namespace, application), flow, flowlet), queue);
  }

  public static FlowletQueueId fromString(String string) {
    return EntityId.fromString(string, FlowletQueueId.class);
  }
}
