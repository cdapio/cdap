/*
 * Copyright © 2020 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.datapipeline.draft;

import io.cdap.cdap.api.NamespaceSummary;

import java.util.Objects;

/**
 * Uniquely identifies a draft.
 */
public class DraftId {
  private final NamespaceSummary namespace;
  private final String id;
  private final String owner;

  public DraftId(NamespaceSummary namespace, String id, String owner) {
    this.namespace = namespace;
    this.id = id;
    this.owner = owner;
  }

  public NamespaceSummary getNamespace() {
    return namespace;
  }

  public String getId() {
    return id;
  }

  public String getOwner() {
    return owner;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DraftId draftId = (DraftId) o;
    return Objects.equals(namespace, draftId.namespace) &&
      Objects.equals(id, draftId.id) &&
      Objects.equals(owner, draftId.owner);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, id, owner);
  }
}
