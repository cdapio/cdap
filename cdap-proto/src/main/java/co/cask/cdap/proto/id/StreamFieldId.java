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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a field of data that belongs to a stream.
 */
public class StreamFieldId extends FieldEntityId implements ParentedId<StreamId> {
  private final String stream;
  private transient Integer hashCode;

  public StreamFieldId(String namespace, String stream, String field) {
    super(namespace, field);
    if (stream == null) {
      throw new NullPointerException("Stream ID cannot be null.");
    }
    ensureValidId("stream", stream);
    this.stream = stream;
  }

  public String getStream() {
    return stream;
  }

  public StreamId getStreamId() {
    return new StreamId(namespace, stream);
  }

  @Override
  public String getEntityName() {
    return getField();
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
    StreamFieldId that = (StreamFieldId) o;
    return Objects.equals(this.namespace, that.namespace) &&
      Objects.equals(this.stream, that.stream) &&
      Objects.equals(this.field, that.field);
  }

  @Override
  public int hashCode() {
    if (hashCode == null) {
      hashCode = Objects.hash(super.hashCode(), namespace, stream, field);
    }
    return hashCode;
  }

  @Override
  public Id.Stream toId() { // no reason to actually implement this...
    return getParent().toId();
  }

  public static StreamFieldId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new StreamFieldId(next(iterator, "namespace"), next(iterator, "stream"),
                              nextAndEnd(iterator, "field"));
  }

  @Override
  protected Iterable<String> toIdParts() {
    return Collections.unmodifiableList(Arrays.asList(namespace, stream, field));
  }
}
