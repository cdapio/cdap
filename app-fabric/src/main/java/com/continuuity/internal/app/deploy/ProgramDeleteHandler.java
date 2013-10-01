package com.continuuity.internal.app.deploy;

import com.continuuity.app.Id;
import com.continuuity.app.program.Type;

/**
 * Call back that is implemented when programs are deleted.
 */
public interface ProgramDeleteHandler {

  /**
   *
   * @param id
   * @param programId
   * @param type
   */
  void process(Id.Account id, Id.Program programId, Type type) throws Exception;

}
