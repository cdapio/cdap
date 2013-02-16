/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.flow.QueueSpecification;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.api.io.Schema;
import com.continuuity.api.io.SchemaTypeAdapter;
import com.continuuity.api.io.UnsupportedTypeException;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.internal.io.QueueSpecificationGeneratorFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.common.io.InputSupplier;
import com.google.common.io.OutputSupplier;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

/**
 * Helper class to encoded/decode {@link ApplicationSpecification} to/from json.
 */
@NotThreadSafe
public final class ApplicationSpecificationAdapter {

  private final QueueSpecificationGeneratorFactory generatorFactory;
  private final Gson gson;

  public static ApplicationSpecificationAdapter create(QueueSpecificationGeneratorFactory generatorFactory) {
    Gson gson = new GsonBuilder()
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .registerTypeAdapter(ApplicationSpecification.class, new ApplicationSpecificationCodec())
      .registerTypeAdapter(FlowSpecification.class, new FlowSpecificationCodec())
      .registerTypeAdapter(FlowletSpecification.class, new FlowletSpecificationCodec())
      .registerTypeAdapter(ProcedureSpecification.class, new ProcedureSpecificationCodec())
      .registerTypeAdapter(QueueSpecification.class, new QueueSpecificationCodec())
      .create();
    return new ApplicationSpecificationAdapter(generatorFactory, gson);
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
    Preconditions.checkState(generatorFactory != null, "No queue generator factory is configured. Fail to serialize to json");
    try {
      for (FlowSpecification flowSpec : appSpec.getFlows().values()) {
        for (FlowletDefinition flowletDef : flowSpec.getFlowlets().values()) {
          flowletDef.generateQueue(generatorFactory.create(flowSpec, flowletDef));
        }
      }
      gson.toJson(appSpec, appendable);

    } catch (UnsupportedTypeException e) {
      throw new IOException(e);
    }
  }

  public void toJson(ApplicationSpecification appSpec,
                     OutputSupplier<? extends Writer> outputSupplier) throws IOException{
    Writer writer = outputSupplier.getOutput();
    try {
      toJson(appSpec, writer);
    } finally {
      Closeables.closeQuietly(writer);
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

  public ApplicationSpecification fromJson(InputSupplier<? extends Reader> inputSupplier) throws IOException {
    Reader reader = inputSupplier.getInput();
    try {
      return fromJson(reader);
    } finally {
      Closeables.closeQuietly(reader);
    }
  }

  private ApplicationSpecificationAdapter(QueueSpecificationGeneratorFactory generatorFactory, Gson gson) {
    this.generatorFactory = generatorFactory;
    this.gson = gson;
  }
}
