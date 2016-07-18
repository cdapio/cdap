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
 * Uniquely identifies a stream.
 */
public class StreamId extends EntityId implements NamespacedId, ParentedId<NamespaceId> {
  private final String namespace;
  private final String stream;
  private transient Integer hashCode;

  public StreamId(String namespace, String stream) {
    super(EntityType.STREAM);
    this.namespace = namespace;
    this.stream = stream;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getStream() {
    return stream;
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
    StreamId streamId = (StreamId) o;
    return Objects.equals(namespace, streamId.namespace) &&
      Objects.equals(stream, streamId.stream);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), namespace, stream);
    }
    return hashCode;
  }

  @Override
  public Id.Stream toId() {
    return Id.Stream.from(namespace, stream);
  }

  @SuppressWarnings("unused")
  public static StreamId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new StreamId(next(iterator, "namespace"), nextAndEnd(iterator, "stream"));
  }

  @Override
  protected Iterable<String> toIdParts() {
    return Collections.unmodifiableList(Arrays.asList(namespace, stream));
  }

  public static StreamId fromString(String string) {
    return EntityId.fromString(string, StreamId.class);
  }

  public StreamViewId view(String view) {
    return new StreamViewId(namespace, stream, view);
  }
}
