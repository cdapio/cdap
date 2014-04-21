/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.api.Application;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.deploy.ConfigResponse;
import com.continuuity.app.deploy.Configurator;
import com.continuuity.app.program.Archive;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.app.Specifications;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.google.common.base.Preconditions;
import com.google.common.io.InputSupplier;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public final class InMemoryConfigurator implements Configurator {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryConfigurator.class);
  /**
   * JAR file path.
   */
  private final Location archive;

  /**
   * Application which needs to be configured.
   */
  private final Application application;

  /**
   * Id of the program
   */
  private final Id.Account id;

  /**
   * Constructor that accepts archive file as input to invoke configure.
   *
   * @param archive name of the archive file for which configure is invoked in-memory.
   */
  public InMemoryConfigurator(Id.Account id, Location archive) {
    Preconditions.checkNotNull(id);
    Preconditions.checkNotNull(archive);
    this.id = id;
    this.archive = archive;
    this.application = null;
  }

  /**
   * Constructor that takes an {@link Application} to invoke configure.
   *
   * @param application instance for which configure needs to be invoked.
   */
  public InMemoryConfigurator(Id.Account id, Application application) {
    Preconditions.checkNotNull(id);
    Preconditions.checkNotNull(application);
    this.id = id;
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
   * This method could be dangerous and should be used only in singlenode.
   * </p>
   *
   * @return A instance of {@link ListenableFuture}.
   */
  @Override
  public ListenableFuture<ConfigResponse> config() {
    StringWriter writer = null;
    SettableFuture result = SettableFuture.create();

    try {
      Application app;

      if (archive != null && application == null) { // Provided Application JAR.
        // Load the JAR using the JAR class load and load the manifest file.
        Object mainClass = new Archive(id, archive).getMainClass().newInstance();
        // Convert it to the type application.
        app = (Application) mainClass;
      } else if (application != null && archive == null) {  // Provided Application instance
        app = application;
      } else {
        throw new IllegalStateException("Have not specified JAR or Application class or have specified both.");
      }

      // Now, we call configure, which returns application specification.
      ApplicationSpecification specification = Specifications.from(app.configure());

      // Convert the specification to JSON.
      // We write the Application specification to output file in JSON format.
      writer = new StringWriter();
      // TODO: The SchemaGenerator should be injected
      ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator()).toJson(specification, writer);
      result.set(new DefaultConfigResponse(0, newStringStream(writer.toString())));

    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      return Futures.immediateFailedFuture(t);
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          LOG.debug(e.getMessage(), e);
          return Futures.immediateFailedFuture(e);
        }
      }
    }
    return result;
  }
}
