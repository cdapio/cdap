/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy.pipeline;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.deploy.ConfigResponse;
import com.continuuity.filesystem.Location;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.app.deploy.InMemoryConfigurator;
import com.continuuity.internal.io.SimpleQueueSpecificationGeneratorFactory;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.pipeline.AbstractStage;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.TimeUnit;

/**
 * LocalArchiveLoaderStage gets a {@link Location} and emits a {@link ApplicationSpecification}.
 * <p>
 *   This stage is responsible for reading the JAR and generating an ApplicationSpecification
 *   that is forwarded to the next stage of processing.
 * </p>
 */
public class LocalArchiveLoaderStage extends AbstractStage<Location> {
  private final ApplicationSpecificationAdapter adapter;

  /**
   * Constructor with hit for handling type.
   */
  public LocalArchiveLoaderStage() {
    super(TypeToken.of(Location.class));
    adapter = ApplicationSpecificationAdapter.create(new SimpleQueueSpecificationGeneratorFactory(new ReflectionSchemaGenerator()));
  }

  /**
   * Creates a {@link com.continuuity.internal.app.deploy.InMemoryConfigurator} to run through
   * the process of generation of {@link ApplicationSpecification}
   *
   * @param archive Location of archive.
   */
  @Override
  public void process(Location archive) throws Exception {
    InMemoryConfigurator inMemoryConfigurator = new InMemoryConfigurator(archive);
    ListenableFuture<ConfigResponse> result = inMemoryConfigurator.config();
    //TODO: Check with Terence on how to handle this stuff.
    ConfigResponse response = result.get(10, TimeUnit.SECONDS);
    ApplicationSpecification specification = adapter.fromJson(response.get());
    emit(new VerificationStage.Input(specification, archive));
  }
}
