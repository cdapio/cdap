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

package co.cask.cdap.test.internal;

import co.cask.cdap.test.ProcedureClient;
import com.google.inject.assistedinject.Assisted;

/**
 * This interface is using Guice assisted inject to create instance of {@link co.cask.cdap.test.ProcedureClient}.
 * @deprecated As of version 2.6.0, with no direct replacement, see {@link co.cask.cdap.api.service.Service}
 */
@Deprecated
public interface ProcedureClientFactory {

  ProcedureClient create(@Assisted("accountId") String accountId, @Assisted("applicationId") String applicationId,
                         @Assisted("procedureName") String procedureName);
}
