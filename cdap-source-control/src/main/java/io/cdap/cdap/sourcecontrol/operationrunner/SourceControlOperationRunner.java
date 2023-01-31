/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.sourcecontrol.operationrunner;

import com.google.common.util.concurrent.ListenableFuture;
import io.cdap.cdap.sourcecontrol.CommitMeta;

import java.io.IOException;
import java.util.List;

public interface SourceControlOperationRunner {
  ListenableFuture<PushAppsResponse> push(List<AppDetailsToPush> appsToPush, CommitMeta commitDetails) throws IOException;

  ListenableFuture<PullAppResponse> pull(String applicationName, String branchName) throws IOException;

  List<ListAppResponse> list();
}
