package com.continuuity.internal.app.runtime;

import com.continuuity.app.program.Program;
import com.google.inject.name.Named;

/**
 * A Guice assisted inject factory for creating different types of {@link DataFabricFacade}.
 */
public interface DataFabricFacadeFactory {

  /**
   * Creates a {@link DataFabricFacade} for the given program, with transaction supports.
   */
  DataFabricFacade create(Program program);

  /**
   * Creates a {@link DataFabricFacade} for the given program, without transaction supports.
   */
  @Named("transaction.off") DataFabricFacade createNoTransaction(Program program);
}
