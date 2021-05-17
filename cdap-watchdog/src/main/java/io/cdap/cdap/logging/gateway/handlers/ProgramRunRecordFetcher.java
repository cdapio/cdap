/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.logging.gateway.handlers;

import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.io.IOException;

/**
 * Interface for fetching {@link RunRecordDetail}
 */
public interface ProgramRunRecordFetcher {
  /**
   * Return {@link RunRecordDetail} for the given {@link ProgramRunId}
   * @param runId for which to fetch {@link RunRecordDetail}
   * @return {@link RunRecordDetail}
   * @throws IOException if failed to fetch the {@link RunRecordDetail}
   * @throws NotFoundException if the program or runid is not found
   */
  RunRecordDetail getRunRecordMeta(ProgramRunId runId) throws IOException, NotFoundException, UnauthorizedException;
}
