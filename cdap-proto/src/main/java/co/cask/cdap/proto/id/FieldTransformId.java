/*
 * Copyright © 2017 Cask Data, Inc.
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

import java.util.Objects;

/**
 * Uniquely identifies a transformation that happened to a field of data.
 */
public class FieldTransformId extends FieldEntityId {
  private final Integer step;
  private transient Integer hashCode;

  protected FieldTransformId(String namespace, String field, int step) {
    super(namespace, field);
    this.step = step;
  }

  public Integer getStep() {
    return step;
  }

  @Override
  public String getEntityName() {
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    FieldTransformId that = (FieldTransformId) o;
    return Objects.equals(this.namespace, that.namespace) &&
      Objects.equals(this.field, that.field) &&
      Objects.equals(this.step, that.step);
  }

  @Override
  public int hashCode() {
    if (hashCode == null) {
      hashCode = Objects.hash(super.hashCode(), namespace, field, step);
    }
    return hashCode;
  }

  @Override
  protected Iterable<String> toIdParts() {
    return null;
  }

  @Override
  public Id.Namespace toId() {
    return null;
  }
}
