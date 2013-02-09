/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.deploy.internal;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.io.Schema;
import com.continuuity.api.io.SchemaTypeAdapter;
import com.continuuity.app.deploy.ConfigResponse;
import com.continuuity.app.deploy.Configurator;
import com.continuuity.classloader.JarClassLoader;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;

/**
 * In Memory Configurator doesn't spawn a external process, but
 * does this in memory.
 */
public class InMemoryConfigurator implements Configurator  {
  /**
   * JAR file path.
   */
  private final File jarFilename;

  /**
   * Constructor.
   * @param jarFilename name of the jar file for which configure is invoked in-memory.
   */
  public InMemoryConfigurator(File jarFilename) {
    Preconditions.checkNotNull(jarFilename);
    this.jarFilename = jarFilename;
  }

  /**
   * Executes the <code>Application.configure</code> within the same JVM.
   * <p>
   *   This method could be dangerous and should be used only in singlenode.
   * </p>
   * @return A instance of {@link ListenableFuture}.
   */
  @Override
  public ListenableFuture<ConfigResponse> config() {
    SettableFuture result = SettableFuture.create();
    try {
      // Load the JAR using the JAR class load and load the manifest file.
      Object mainClass;
      JarClassLoader loader = new JarClassLoader(jarFilename.getAbsolutePath());
      mainClass = loader.getMainClass(Application.class);

      // Convert it to the type application.
      Application application = (Application) mainClass;

      // Now, we call configure, which returns application specification.
      ApplicationSpecification specification = application.configure();

      // Convert the specification to JSON.
      Gson gson = new GsonBuilder()
                    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
                    .create();

      // We write the Application specification to output file in JSON format.
      StringWriter writer = new StringWriter();
      gson.toJson(specification, writer);
      result.set(new DefaultConfigResponse(0, new StringReader(writer.toString())));
    } catch (Exception e) {
      Futures.immediateFailedFuture(e);
    }
    return result;
  }
}
