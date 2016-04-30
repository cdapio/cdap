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
 * Uniquely identifies a schedule.
 */
public class ScheduleId extends EntityId implements NamespacedId, ParentedId<ApplicationId> {
  private final String namespace;
  private final String application;
  private final String schedule;

  public ScheduleId(String namespace, String application, String schedule) {
    super(EntityType.SCHEDULE);
    this.namespace = namespace;
    this.application = application;
    this.schedule = schedule;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getApplication() {
    return application;
  }

  public String getSchedule() {
    return schedule;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    ScheduleId that = (ScheduleId) o;
    return Objects.equals(namespace, that.namespace) &&
      Objects.equals(application, that.application) &&
      Objects.equals(schedule, that.schedule);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), namespace, application, schedule);
  }

  @Override
  public ApplicationId getParent() {
    return new ApplicationId(namespace, application);
  }

  @Override
  public Id.Schedule toId() {
    return Id.Schedule.from(Id.Application.from(namespace, application), schedule);
  }

  @SuppressWarnings("unused")
  public static ScheduleId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new ScheduleId(
      next(iterator, "namespace"), next(iterator, "application"),
      nextAndEnd(iterator, "schedule"));
  }

  @Override
  protected Iterable<String> toIdParts() {
    return Collections.unmodifiableList(Arrays.asList(namespace, application, schedule));
  }

  public static ScheduleId fromString(String string) {
    return EntityId.fromString(string, ScheduleId.class);
  }
}
