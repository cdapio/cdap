/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.io.Schema;
import com.continuuity.api.io.SchemaGenerator;
import com.continuuity.api.io.SchemaTypeAdapter;
import com.continuuity.api.io.UnsupportedTypeException;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.Reader;

/**
 * Helper class to encoded/decode {@link ApplicationSpecification} to/from json.
 */
@NotThreadSafe
public final class ApplicationSpecificationAdapter {

  private final SchemaGenerator schemaGenerator;
  private final Gson gson;

  public static ApplicationSpecificationAdapter create(SchemaGenerator generator) {
    Gson gson = new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
    return new ApplicationSpecificationAdapter(generator, gson);
  }

  public static ApplicationSpecificationAdapter create() {
    return create(null);
  }

  public String toJson(ApplicationSpecification appSpec) {
    try {
      StringBuilder builder = new StringBuilder();
      toJson(appSpec, builder);
      return builder.toString();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public void toJson(ApplicationSpecification appSpec, Appendable appendable) throws IOException {
    Preconditions.checkState(schemaGenerator != null, "No schema generator is configured. Fail to serialize to json");
    try {
      for (FlowSpecification flowSpec : appSpec.getFlows().values()) {
        for (FlowletDefinition flowletDef : flowSpec.getFlowlets().values()) {
          flowletDef.generateSchema(schemaGenerator);
        }
      }
      gson.toJson(appSpec, appendable);

    } catch (UnsupportedTypeException e) {
      throw new IOException(e);
    }
  }

  public ApplicationSpecification fromJson(String json) {
    return gson.fromJson(json, ApplicationSpecification.class);
  }

  public ApplicationSpecification fromJson(Reader reader) throws IOException {
    try {
      return gson.fromJson(reader, ApplicationSpecification.class);
    } catch (JsonParseException e) {
      throw new IOException(e);
    }
  }

  private ApplicationSpecificationAdapter(SchemaGenerator schemaGenerator, Gson gson) {
    this.schemaGenerator = schemaGenerator;
    this.gson = gson;
  }
}
