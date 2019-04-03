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
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;

/**
 * Loads system artifacts.
 */
public class SystemArtifactLoader extends BaseStepExecutor<EmptyArguments> {
  private final ArtifactRepository artifactRepository;

  @Inject
  SystemArtifactLoader(ArtifactRepository artifactRepository) {
    this.artifactRepository = artifactRepository;
  }

  @Override
  public void execute(EmptyArguments arguments) {
    try {
      artifactRepository.addSystemArtifacts();
    } catch (Exception e) {
      // retrying everything was the behavior before this logic got moved to the bootstrap framework.
      // if there really is non-transient exception here, the retry time limit will eventually be hit
      // and this step will be skipped.
      throw new RetryableException(e);
    }
  }
}
