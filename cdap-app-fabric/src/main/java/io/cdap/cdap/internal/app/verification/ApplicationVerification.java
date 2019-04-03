/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.verification;

import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.app.verification.AbstractVerifier;
import io.cdap.cdap.app.verification.VerifyResult;
import io.cdap.cdap.error.Err;
import io.cdap.cdap.proto.id.ApplicationId;

import java.util.Arrays;

/**
 * This class is responsible for verifying the Application details of
 * the {@link ApplicationSpecification}.
 * <p/>
 * <p>
 * Following are the checks done for Application
 * <ul>
 * <li>Application name is an ID</li>
 * <li>Application contains at least one program</li>
 * </ul>
 * </p>
 */
public class ApplicationVerification extends AbstractVerifier<ApplicationSpecification> {

  /**
   * Verifies {@link ApplicationSpecification} being provide.
   *
   * @param input to be verified
   * @return An instance of {@link VerifyResult} depending of status of verification.
   */
  @Override
  public VerifyResult verify(ApplicationId appId, final ApplicationSpecification input) {
    VerifyResult verifyResult = super.verify(appId, input);

    if (!verifyResult.isSuccess()) {
      return verifyResult;
    }

    // Check if there is at least one program
    // Loop through all program types. For each program type, get the number of programs of that type.
    // Then sum up total number of programs.
    int numberOfPrograms = Arrays.stream(ProgramType.values())
      .mapToInt(t -> input.getProgramsByType(t).size())
      .reduce(0, (l, r) -> l + r);
    if (numberOfPrograms <= 0) {
      return VerifyResult.failure(Err.Application.ATLEAST_ONE_PROCESSOR, input.getName());
    }

    return VerifyResult.success();
  }

  @Override
  protected String getName(ApplicationSpecification input) {
    return input.getName();
  }
}
