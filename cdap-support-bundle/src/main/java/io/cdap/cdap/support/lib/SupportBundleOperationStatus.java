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

package io.cdap.cdap.support.lib;

import io.cdap.cdap.support.status.CollectionState;

public class SupportBundleOperationStatus {
  private final String bundleId;
  private CollectionState bundleStatus;
  private SupportBundlePipelineStatus supportBundlePipelineStatus;

  public SupportBundleOperationStatus(String bundleId) {
    this.bundleId = bundleId;
  }

  public String getBundleId() {
    return bundleId;
  }

  public CollectionState getBundleStatus() {
    return bundleStatus;
  }

  public void setBundleStatus(CollectionState bundleStatus) {
    this.bundleStatus = bundleStatus;
  }

  public SupportBundlePipelineStatus getSupportBundlePipelineStatus() {
    return supportBundlePipelineStatus;
  }

  public void setSupportBundlePipelineStatus(SupportBundlePipelineStatus supportBundlePipelineStatus) {
    this.supportBundlePipelineStatus = supportBundlePipelineStatus;
  }
}
