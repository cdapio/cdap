/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.deploy.ConfigResponse;
import com.continuuity.app.deploy.Configurator;
import com.continuuity.app.program.ProgramArchive;
import com.continuuity.filesystem.Location;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.io.SimpleQueueSpecificationGeneratorFactory;
import com.google.common.base.Preconditions;
import com.google.common.io.InputSupplier;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;

/**
 * In Memory Configurator doesn't spawn a external process, but
 * does this in memory.
 *
 * @see SandboxConfigurator
 */
public class InMemoryConfigurator implements Configurator  {
  /**
   * JAR file path.
   */
  private final Location archive;

  /**
   * Application which needs to be configured.
   */
  private final Application application;

  /**
   * Constructor that accepts archive file as input to invoke configure.
   * @param archive name of the archive file for which configure is invoked in-memory.
   */
  public InMemoryConfigurator(Location archive) {
    Preconditions.checkNotNull(archive);
    this.archive = archive;
    this.application = null;
  }

  /**
   * Constructor that takes an {@link Application} to invoke configure.
   * @param application instance for which configure needs to be invoked.
   */
  public InMemoryConfigurator(Application application) {
    Preconditions.checkNotNull(application);
    this.archive = null;
    this.application = application;
  }

  /**
   * Returns a {@link Reader} for the string.
   *
   * @param result to be converted into {@link Reader}
   * @return An instance of {@link Reader}
   */
  private InputSupplier<Reader> newStringStream(final String result) {
    return new InputSupplier<Reader>() {
      @Override
      public Reader getInput() throws IOException {
        return new StringReader(result);
      }
    };
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
    StringWriter writer = null;
    SettableFuture result = SettableFuture.create();

    try {
      Application app = null;

      if(archive != null && application == null) { // Provided Application JAR.
        // Load the JAR using the JAR class load and load the manifest file.
        Object mainClass = new ProgramArchive(archive).getMainClass().newInstance();
        // Convert it to the type application.
        app  = (Application) mainClass;
      } else if(application != null && archive == null) {  // Provided Application instance
        app = application;
      } else {
        throw new IllegalStateException("Have not specified JAR or Application class or have specified both.");
      }

      // Now, we call configure, which returns application specification.
      ApplicationSpecification specification = app.configure();

      // Convert the specification to JSON.
      // We write the Application specification to output file in JSON format.
      writer = new StringWriter();
      // TODO: The SchemaGenerator should be injected
      ApplicationSpecificationAdapter.create(SimpleQueueSpecificationGeneratorFactory.create()).toJson(specification, writer);
      result.set(new DefaultConfigResponse(0, newStringStream(writer.toString())));
    } catch (Exception e) {
      return Futures.immediateFailedFuture(e);
    } catch (Throwable throwable) {
      return Futures.immediateFailedFuture(throwable);
    } finally {
      if(writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          Futures.immediateFailedFuture(e);
        }
      }
    }
    return result;
  }
}
