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

package co.cask.cdap.proto.id;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.element.EntityType;

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a CDAP instance.
 */
public class InstanceId extends EntityId {
  private final String instance;
  private transient Integer hashCode;

  public InstanceId(String instance) {
    super(EntityType.INSTANCE);
    if (instance == null) {
      throw new NullPointerException("Instance ID cannot be null");
    }
    this.instance = instance;
  }

  public String getInstance() {
    return instance;
  }

  @Override
  public String getEntityName() {
    return getInstance();
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.singletonList(instance);
  }

  @Override
  public Id toId() {
    throw new UnsupportedOperationException(
      String.format("%s does not have old %s class", InstanceId.class.getName(), Id.class.getName()));
  }

  @SuppressWarnings("unused")
  public static InstanceId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new InstanceId(nextAndEnd(iterator, "instance"));
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    InstanceId other = (InstanceId) o;
    return Objects.equals(instance, other.instance);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), instance);
    }
    return hashCode;
  }
}
