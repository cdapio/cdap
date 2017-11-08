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
 * Uniquely identifies a stream view.
 */
public class StreamViewId extends NamespacedEntityId implements ParentedId<StreamId> {
  private final String stream;
  private final String view;
  private transient Integer hashCode;

  public StreamViewId(String namespace, String stream, String view) {
    super(namespace, EntityType.STREAM_VIEW);
    if (stream == null) {
      throw new NullPointerException("Stream ID cannot be null.");
    }
    if (view == null) {
      throw new NullPointerException("View ID cannot be null.");
    }
    ensureValidId("stream", stream);
    ensureValidId("view", view);
    this.stream = stream;
    this.view = view;
  }

  public String getStream() {
    return stream;
  }

  public String getView() {
    return view;
  }

  @Override
  public String getEntityName() {
    return getView();
  }

  @Override
  public StreamId getParent() {
    return new StreamId(namespace, stream);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    StreamViewId that = (StreamViewId) o;
    return Objects.equals(namespace, that.namespace) &&
      Objects.equals(stream, that.stream) &&
      Objects.equals(view, that.view);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), namespace, stream, view);
    }
    return hashCode;
  }

  @Override
  public Id.Stream.View toId() {
    return Id.Stream.View.from(namespace, stream, view);
  }

  @SuppressWarnings("unused")
  public static StreamViewId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new StreamViewId(
      next(iterator, "namespace"), next(iterator, "stream"),
      nextAndEnd(iterator, "view"));
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.unmodifiableList(Arrays.asList(namespace, stream, view));
  }

  public static StreamViewId fromString(String string) {
    return EntityId.fromString(string, StreamViewId.class);
  }
}
