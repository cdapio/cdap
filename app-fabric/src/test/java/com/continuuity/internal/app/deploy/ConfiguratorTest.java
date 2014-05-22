/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.WordCountApp;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.deploy.ConfigResponse;
import com.continuuity.app.deploy.Configurator;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.test.internal.AppFabricTestHelper;
import com.continuuity.test.internal.DefaultId;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests the configurators.
 *
 * NOTE: Till we can build the JAR it's difficult to test other configurators
 * {@link com.continuuity.internal.app.deploy.InMemoryConfigurator} &
 * {@link com.continuuity.internal.app.deploy.SandboxConfigurator}
 */
public class ConfiguratorTest {

  @Test
  public void testInMemoryConfigurator() throws Exception {
    Location appJar = AppFabricTestHelper.createAppJar(WordCountApp.class);

    // Create a configurator that is testable. Provide it a application.
    Configurator configurator = new InMemoryConfigurator(DefaultId.ACCOUNT, appJar);

    // Extract response from the configurator.
    ListenableFuture<ConfigResponse> result = configurator.config();
    ConfigResponse response = result.get(10, TimeUnit.SECONDS);
    Assert.assertNotNull(response);

    // Deserialize the JSON spec back into Application object.
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification specification = adapter.fromJson(response.get());
    Assert.assertNotNull(specification);
    Assert.assertTrue(specification.getName().equals("WordCountApp")); // Simple checks.
    Assert.assertTrue(specification.getFlows().size() == 1); // # of flows.
  }

}
