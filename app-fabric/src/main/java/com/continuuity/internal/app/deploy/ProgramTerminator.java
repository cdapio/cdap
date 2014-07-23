/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.proto.Id;
import com.continuuity.proto.ProgramType;

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
  void stop (Id.Account id, Id.Program programId, ProgramType type) throws Exception;

}
