/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.internal.app.runtime.procedure.ProcedureProgramRunner;

/**
 *
 */
final class ProcedureWeaveRunnable extends AbstractProgramWeaveRunnable<ProcedureProgramRunner> {

  ProcedureWeaveRunnable(String name, String hConfName, String cConfName) {
    super(name, hConfName, cConfName);
  }

  @Override
  protected Class<ProcedureProgramRunner> getProgramClass() {
    return ProcedureProgramRunner.class;
  }
}
