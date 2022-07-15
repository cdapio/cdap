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

package io.cdap.cdap.app.runtime;

import com.google.inject.Inject;
import io.cdap.cdap.common.twill.TwillAppNames;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.internal.RunIds;

import java.util.Objects;

/**
 * Implementation of {@link TwillControllerCreator} for On-Premise(Native) Cluster mode.
 */
public class NativeTwillControllerCreator implements TwillControllerCreator {

  private final TwillRunnerService twillRunnerService;

  @Inject
  public NativeTwillControllerCreator(TwillRunnerService twillRunnerService) {
    this.twillRunnerService = twillRunnerService;
  }

  @Override
  public TwillController createTwillController(ProgramRunId programRunId) {
    return twillRunnerService.lookup(TwillAppNames.toTwillAppName(programRunId.getParent()),
                                     RunIds.fromString(Objects.requireNonNull(programRunId.getRun())));
  }
}
