/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.notifications;

import co.cask.cdap.notifications.service.NotificationFeedException;
import com.google.common.base.Objects;

/**
 * Notification Feed POJO.
 */
public class NotificationFeed {
  private final String id;
  private final String category;
  private final String namespace;
  private final String name;
  private final String description;

  public static NotificationFeed fromId(String id) throws NotificationFeedException {
    String[] idParts = id.split("\\.");
    if (idParts.length != 3) {
      throw new NotificationFeedException(String.format("Id %s is not a valid feed id.", id));
    }
    return new NotificationFeed(idParts[0], idParts[1], idParts[2], "");
  }

  private NotificationFeed(String namespace, String category, String name, String description) {
    this.namespace = namespace;
    this.category = category;
    this.name = name;
    this.id = String.format("%s.%s.%s", namespace, category, name);
    this.description = description;
  }

  public String getCategory() {
    return category;
  }

  public String getId() {
    return id;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  /**
   * Builder used to build {@link NotificationFeed}.
   */
  public static final class Builder {
    private String category;
    private String name;
    private String namespace;
    private String description;

    public Builder setName(final String name) {
      this.name = name;
      return this;
    }

    public Builder setNamespace(final String namespace) {
      this.namespace = namespace;
      return this;
    }

    public Builder setDescription(final String description) {
      this.description = description;
      return this;
    }

    public Builder setCategory(final String category) {
      this.category = category;
      return this;
    }

    public NotificationFeed build() {
      return new NotificationFeed(namespace, category, name, description);
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("id", id)
      .add("description", description)
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    NotificationFeed that = (NotificationFeed) o;
    return Objects.equal(this.id, that.id) &&
      Objects.equal(this.category, that.category) &&
      Objects.equal(this.namespace, that.namespace) &&
      Objects.equal(this.name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, category, namespace, name);
  }
}
