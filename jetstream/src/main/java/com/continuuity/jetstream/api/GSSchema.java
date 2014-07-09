package com.continuuity.jetstream.api;

import com.continuuity.jetstream.internal.DefaultGSSchema;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.LinkedHashMap;
import java.util.Set;

/**
 *
 */
public interface GSSchema {

  String getName();

  /**
   * Builder for creating instance of {@link GSSchema}. The builder instance is not reusable, meaning
   * each instance of this class can only be used to create on instance of {@link GSSchema}.
   */
  static final class Builder {
    private String name;
    private LinkedHashMap<String, PrimitiveType> fieldNames = Maps.newLinkedHashMap();
    private Set<String> increasingFields = Sets.newHashSet();
    private Set<String> decreasingFields = Sets.newHashSet();

    public static NameSetter with() {
      return new Builder().new NameSetter();
    }

    public final class NameSetter {

      public FieldSetter setName(String name) {
        Preconditions.checkArgument(name != null, "Name cannot be null.");
        Builder.this.name = name;
        return new FieldSetter();
      }
    }

    public final class FieldSetter {

      private void fieldCheck(String name) {
        Preconditions.checkArgument(name != null, "Field name cannot be null.");
        Preconditions.checkState(!fieldNames.containsKey(name), "Field name already used.");
      }

      public FieldSetter addField(String name, PrimitiveType type) {
        fieldCheck(name);
        Builder.this.fieldNames.put(name, type);
        return this;
      }

      public FieldSetter addIncreasingField(String name, PrimitiveType type) {
        fieldCheck(name);
        Builder.this.fieldNames.put(name, type);
        Builder.this.increasingFields.add(name);
        return this;
      }

      public FieldSetter addDecreasingField(String name, PrimitiveType type) {
        fieldCheck(name);
        Builder.this.fieldNames.put(name, type);
        Builder.this.decreasingFields.add(name);
        return this;
      }

      public GSSchema build() {
        return new DefaultGSSchema(name, fieldNames, increasingFields, decreasingFields);
      }
    }

    private Builder() {
    }

  }

}
