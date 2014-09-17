/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.app.deploy;

import co.cask.cdap.internal.app.deploy.ProgramTerminator;

/**
 * Factory for creating deployment {@link Manager}.
 *
 * @param <I> Input type of the deployment.
 * @param <O> Output type of the deployment.
 */
public interface ManagerFactory<I, O> {
  Manager<I, O> create(ProgramTerminator handler);
}
