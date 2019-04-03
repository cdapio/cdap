/*
 * Copyright © 2018 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.internal.bootstrap.executor;

import com.google.inject.Inject;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.internal.profile.ProfileService;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.profile.Profile;

import java.util.concurrent.TimeUnit;

/**
 * Creates the native profile if it doesn't exist.
 */
public class NativeProfileCreator extends BaseStepExecutor<EmptyArguments> {
  private final ProfileService profileService;

  @Inject
  NativeProfileCreator(ProfileService profileService) {
    this.profileService = profileService;
  }

  @Override
  public void execute(EmptyArguments arguments) {
    try {
      profileService.createIfNotExists(ProfileId.NATIVE, Profile.NATIVE);
    } catch (RuntimeException e) {
      // the native profile is valid. Any exception here is transient and should be retried.
      throw new RetryableException(e);
    }
  }

  @Override
  protected RetryStrategy getRetryStrategy() {
    // retry with no time limit
    return RetryStrategies.exponentialDelay(200, 10000, TimeUnit.MILLISECONDS);
  }
}
