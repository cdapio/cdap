/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.proto.element.ElementType;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a particular instance of an element.
 *
 * <p>
 *   When adding a new type of {@link ElementId}, the following must be done:
 *   <ol>
 *     <li>
 *       Implement interfaces
 *       <ol>
 *         <li>{@link NamespacedId} if the new ID belongs to a namespace</li>
 *         <li>{@link ParentedId} if the new ID has a parent ID</li>
 *       </ol>
 *     </li>
 *     <li>
 *       Add methods
 *       <ol>
 *         <li>{@code equals()} and {@code hashCode()}, using the implementations in {@link ElementId}</li>
 *         <li>{@code fromString(String string)}, delegating to {@link ElementId#fromString(String, Class)}</li>
 *       </ol>
 *     </li>
 *     <li>
 *       Create a corresponding child class of the old {@link Id}
 *       (once {@link Id} is removed, this is no longer needed)
 *     </li>
 *     <li>
 *       Add a new entry to {@link ElementType}, associating the {@link ElementType}
 *       with both the new {@link ElementId} and old {@link Id}
 *     </li>
 *   </ol>
 * </p>
 */
@SuppressWarnings("unchecked")
public abstract class ElementId implements IdCompatible {

  private static final String IDSTRING_TYPE_SEPARATOR = ":";
  private static final String IDSTRING_PART_SEPARATOR = ".";

  private final ElementType elementType;

  protected ElementId(ElementType elementType) {
    this.elementType = elementType;
  }

  protected abstract Iterable<String> toIdParts();

  public final ElementType getElementType() {
    return elementType;
  }

  public static <T extends Id> T fromStringOld(String string, Class<T> oldIdClass) {
    ElementType type = ElementType.valueOfOldIdClass(oldIdClass);
    ElementId id = fromString(string, type.getIdClass());
    return (T) id.toId();
  }

  protected static <T extends ElementId> T fromString(String string, Class<T> idClass) {
    String[] typeAndId = string.split(IDSTRING_TYPE_SEPARATOR, 2);
    Preconditions.checkArgument(
      typeAndId.length == 2,
      "Expected type separator '%s' to be in the ID string: %s", IDSTRING_TYPE_SEPARATOR, string);

    String typeString = typeAndId[0];
    ElementType type = ElementType.valueOf(typeString.toUpperCase());
    Preconditions.checkArgument(type != null, "Invalid element type: " + typeString);
    Preconditions.checkArgument(
      type.getIdClass().equals(idClass),
      "Expected ElementId of class '%s' but got '%s'",
      idClass.getName(), type.getIdClass().getName());

    String idString = typeAndId[1];
    try {
      return type.fromIdParts(Splitter.on(IDSTRING_PART_SEPARATOR).split(idString));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
        String.format("Invalid ID for type '%s': %s", idClass.getName(), string), e);
    }
  }

  @Override
  public final String toString() {
    StringBuilder result = new StringBuilder();
    result.append(elementType.name().toLowerCase()).append(IDSTRING_TYPE_SEPARATOR);
    Iterable<String> idParts = toIdParts();
    for (String part : idParts) {
      result.append(part).append(IDSTRING_PART_SEPARATOR);
    }
    if (!Iterables.isEmpty(idParts)) {
      result.deleteCharAt(result.length() - 1);
    }
    return result.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ElementId elementId = (ElementId) o;
    return Objects.equals(elementType, elementId.elementType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(elementType);
  }

  protected static String next(Iterator<String> iterator, String fieldName) {
    Preconditions.checkArgument(iterator.hasNext(), "Missing field: %s", fieldName);
    return iterator.next();
  }

  protected static String nextAndEnd(Iterator<String> iterator, String fieldName) {
    String result = next(iterator, fieldName);
    if (iterator.hasNext()) {
      throw new IllegalArgumentException(
        String.format("Expected end after field '%s' but got: %s", fieldName, remaining(iterator)));
    }
    return result;
  }

  protected static String remaining(Iterator<String> iterator, String fieldName) {
    Preconditions.checkArgument(iterator.hasNext(), "Missing field: %s", fieldName);
    StringBuilder result = new StringBuilder();
    while (iterator.hasNext()) {
      result.append(iterator.next()).append(IDSTRING_PART_SEPARATOR);
    }
    if (result.length() != 0) {
      result.deleteCharAt(result.length() - 1);
    }
    return result.toString();
  }

  private static String remaining(Iterator<String> iterator) {
    return Joiner.on(IDSTRING_PART_SEPARATOR).join(iterator);
  }
}
