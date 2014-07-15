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

package com.continuuity.api.procedure;

import com.continuuity.api.ProgramLifecycle;

/**
 * This interface defines a Procedure.
 */
public interface Procedure extends ProgramLifecycle<ProcedureContext> {

  /**
   * Configures this procedure providing a specification with more details about the procedure.
   * <p>
   *   To create a Procedure, one must implement this interface. This method will be
   *   invoked during deployment time and it returns a {@link ProcedureSpecification} to specify how to
   *   configure the given procedure. There are no guarantees around how many times this method
   *   will be called during deployment or runtime, hence, the configuration should be very simple and should
   *   not include initialization of resources.
   * </p>
   * @return An instance of {@link ProcedureSpecification}
   * @see ProcedureSpecification
   */
  ProcedureSpecification configure();

  /**
   * Initializes this Procedure at run-time.
   * <p>
   *  This method is invoked only once during startup of Procedure on a per instance basis. This method can be
   *  be used to initialize any user related resources.
   * </p>
   * @param context procedure runtime context
   */
  @Override
  void initialize(ProcedureContext context);

  /**
   * Invoked after the Procedure has been stopped.
   * <p>
   *   Upon stopping a Procedure, no more requests will be handled by this instance of Procedure.
   *   This method will be invoked after stopping incoming requests and closing {@link ProcedureResponder}
   * </p>
   */
  @Override
  void destroy();
}
