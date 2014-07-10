package com.continuuity.jetstream.api;

import com.continuuity.jetstream.internal.DefaultGSSchema;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.LinkedHashMap;
import java.util.Set;

/**
 * Used to define the Schema of a Input Stream to {@link com.continuuity.jetstream.api.AbstractGSFlowlet}.
 */
public interface GSSchema {

  LinkedHashMap<String, PrimitiveType> getFieldNames();

  Set<String> getIncreasingFields();

  Set<String> getDecreasingFields();

  /**
   * Builder for creating instance of {@link GSSchema}. The builder instance is not reusable, meaning
   * each instance of this class can only be used to create on instance of {@link GSSchema}.
   */
  static final class Builder {
    private LinkedHashMap<String, PrimitiveType> fieldNames = Maps.newLinkedHashMap();
    private Set<String> increasingFields = Sets.newHashSet();
    private Set<String> decreasingFields = Sets.newHashSet();

    public static FieldSetter with() {
      return new Builder().new FieldSetter();
    }

    public final class FieldSetter {

      private void fieldCheck(String name) {
        Preconditions.checkArgument(name != null, "Field name cannot be null.");
        Preconditions.checkState(!fieldNames.containsKey(name), "Field name already used.");
      }

      public FieldSetter field(String name, PrimitiveType type) {
        fieldCheck(name);
        Builder.this.fieldNames.put(name, type);
        return this;
      }

      public FieldSetter increasingField(String name, PrimitiveType type) {
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
        //TODO: Add a check to make sure there is at least one increasing or decreasing fields.
        return new DefaultGSSchema(fieldNames, increasingFields, decreasingFields);
      }
    }

    private Builder() {
    }

  }

}
