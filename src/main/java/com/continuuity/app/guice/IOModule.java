package com.continuuity.app.guice;

import com.continuuity.internal.io.ASMDatumWriterFactory;
import com.continuuity.internal.io.ASMFieldAccessorFactory;
import com.continuuity.internal.io.DatumWriterFactory;
import com.continuuity.internal.io.FieldAccessorFactory;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;

/**
 * A Guice module for IO related classes bindings.
 */
public class IOModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(FieldAccessorFactory.class).to(ASMFieldAccessorFactory.class).in(Scopes.SINGLETON);
    bind(DatumWriterFactory.class).to(ASMDatumWriterFactory.class).in(Scopes.SINGLETON);

    expose(DatumWriterFactory.class);
  }
}
