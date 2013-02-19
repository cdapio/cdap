/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity;

import com.continuuity.app.deploy.Manager;
import com.continuuity.app.program.ManifestFields;
import com.continuuity.app.program.Store;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Configuration;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.filesystem.Location;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.internal.app.deploy.LocalManager;
import com.continuuity.internal.app.program.MDSBasedStore;
import com.continuuity.internal.filesystem.LocalLocationFactory;
import com.continuuity.internal.pipeline.SynchronousPipelineFactory;
import com.continuuity.metadata.thrift.MetadataService;
import com.continuuity.pipeline.PipelineFactory;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * This is a test helper for our internal test.
 * <p>
 *   <i>Note: please don't include this in the developer test</i>
 * </p>
 */
public class TestHelper {

  /**
   * Given a class generates a manifest file with main-class as class.
   *
   * @param klass to set as Main-Class in manifest file.
   * @return An instance {@link Manifest}
   */
  public static Manifest getManifestWithMainClass(Class<?> klass) {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(ManifestFields.MAIN_CLASS, klass.getCanonicalName());
    return manifest;
  }

  /**
   * @return Returns an instance of {@link LocalManager}
   */
  public static Manager<Location, String> getLocalManager(CConfiguration configuration) {
    LocationFactory lf = new LocalLocationFactory();
    PipelineFactory pf = new SynchronousPipelineFactory();

    final Injector injector =
      Guice.createInjector(
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(OperationExecutor.class).to(NoOperationExecutor.class);
            bind(MetadataService.Iface.class).to(com.continuuity.metadata.MetadataService.class);
            bind(MetaDataStore.class).to(SerializingMetaDataStore.class);
          }
        }
      );

    Store store = injector.getInstance(MDSBasedStore.class);
    return new LocalManager(configuration, pf, lf, store);
  }
}
