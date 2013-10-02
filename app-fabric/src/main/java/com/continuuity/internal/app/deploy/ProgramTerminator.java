package com.continuuity.internal.app.deploy;

import com.continuuity.app.Id;
import com.continuuity.app.program.Type;

/**
 * Interface that is responsible to stopping programs. Used while stop programs that are being deleted during
 * re-deploy process.
 */
public interface ProgramTerminator {

  /**
   * Method to implement for stopping the programs.
   *
   * @param id         Account id.
   * @param programId  Program id.
   * @param type       Program Type.
   */
  void stop (Id.Account id, Id.Program programId, Type type) throws Exception;

}
