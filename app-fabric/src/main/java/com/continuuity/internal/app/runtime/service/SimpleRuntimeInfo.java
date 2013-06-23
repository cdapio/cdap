package com.continuuity.internal.app.runtime.service;

import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramRuntimeService;

/**
 *
 */
public final class SimpleRuntimeInfo implements ProgramRuntimeService.RuntimeInfo {

  private final ProgramController controller;
  private final Type type;
  private final Id.Program programId;

  public SimpleRuntimeInfo(ProgramController controller, Program program) {
    this(controller,
         program.getProcessorType(),
         Id.Program.from(program.getAccountId(), program.getApplicationId(), program.getProgramName()));
  }

  public SimpleRuntimeInfo(ProgramController controller, Type type, Id.Program programId) {
    this.controller = controller;
    this.type = type;
    this.programId = programId;
  }

  @Override
  public ProgramController getController() {
    return controller;
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public Id.Program getProgramId() {
    return programId;
  }
}
