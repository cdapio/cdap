/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.deploy.ConfigResponse;
import com.continuuity.filesystem.Location;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.pipeline.AbstractStage;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.TimeUnit;

/**
 * LocalArchiveLoaderStage gets a {@link Location} and emits a {@link ApplicationSpecification}.
 */
public class LocalArchiveLoaderStage extends AbstractStage<Location> {
  private final ApplicationSpecificationAdapter adapter;

  /**
   * Constructor with hit for handling type.
   */
  public LocalArchiveLoaderStage() {
    super(TypeToken.of(Location.class));
    adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
  }

  /**
   * Creates a {@link InMemoryConfigurator} to run through the process of generation
   * of {@link ApplicationSpecification}
   *
   * @param o Location of archive.
   */
  @Override
  public void process(Location o) throws Exception {
    InMemoryConfigurator inMemoryConfigurator = new InMemoryConfigurator(o);
    ListenableFuture<ConfigResponse> result = inMemoryConfigurator.config();

    // Blocked wait.
    ConfigResponse response = result.get(10, TimeUnit.SECONDS);
    ApplicationSpecification specification = adapter.fromJson(response.get());
    emit(specification);
  }
}
