/*
 * Copyright © 2020 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.datapipeline.service;

import io.cdap.cdap.api.artifact.ArtifactSummary;

import javax.annotation.Nullable;

/**
 * Helper functions for handlers/services in {@link io.cdap.cdap.datapipeline.service.StudioService}
 */
public final class StudioUtil {
  public static final String ARTIFACT_BATCH_NAME = "cdap-data-pipeline";
  public static final String ARTIFACT_STREAMING_NAME = "cdap-data-streams";

  private StudioUtil() {
    // prevent instantiation of util class.
  }

  public static boolean isBatchPipeline(@Nullable ArtifactSummary artifactSummary) {
    if (artifactSummary == null) {
      return false;
    }
    return ARTIFACT_BATCH_NAME.equals(artifactSummary.getName());
  }

  public static boolean isStreamingPipeline(@Nullable ArtifactSummary artifactSummary) {
    if (artifactSummary == null) {
      return false;
    }
    return ARTIFACT_STREAMING_NAME.equals(artifactSummary.getName());
  }
}
