/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.proto.id.ProgramRunId;

import java.util.concurrent.ScheduledExecutorService;

/**
 * RemoteExecutionServiceFactory interface which is to initialize an instance of {@link ExecutionService}.
 */
public interface ExecutionServiceFactory {
  ExecutionService create(CConfiguration cConf,
                          ProgramRunId programRunId,
                          ScheduledExecutorService scheduler,
                          RemoteProcessController processController,
                          ProgramStateWriter programStateWriter);
}
