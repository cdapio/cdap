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
 * Uniquely identifies a notification feed.
 */
public class NotificationFeedId extends NamespacedEntityId implements ParentedId<NamespaceId> {
  private final String category;
  private final String feed;
  private transient Integer hashCode;

  public NotificationFeedId(String namespace, String category, String feed) {
    super(namespace, EntityType.NOTIFICATION_FEED);
    if (category == null) {
      throw new NullPointerException("Category cannot be null.");
    }
    if (feed == null) {
      throw new NullPointerException("Feed ID cannot be null.");
    }
    ensureValidId("category", category);
    ensureValidId("feed", feed);
    this.category = category;
    this.feed = feed;
  }

  public String getCategory() {
    return category;
  }

  public String getFeed() {
    return feed;
  }

  @Override
  public String getEntityName() {
    return getFeed();
  }

  @Override
  public NamespaceId getParent() {
    return new NamespaceId(namespace);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    NotificationFeedId that = (NotificationFeedId) o;
    return Objects.equals(namespace, that.namespace) &&
      Objects.equals(category, that.category) &&
      Objects.equals(feed, that.feed);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), namespace, category, feed);
    }
    return hashCode;
  }

  @Override
  public Id.NotificationFeed toId() {
    return Id.NotificationFeed.from(namespace, category, feed);
  }

  @SuppressWarnings("unused")
  public static NotificationFeedId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new NotificationFeedId(
      next(iterator, "namespace"), next(iterator, "category"),
      nextAndEnd(iterator, "feed"));
  }

  @Override
  protected Iterable<String> toIdParts() {
    return Collections.unmodifiableList(Arrays.asList(namespace, category, feed));
  }

  public static NotificationFeedId fromString(String string) {
    return EntityId.fromString(string, NotificationFeedId.class);
  }
}
