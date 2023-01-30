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

package io.cdap.cdap.sourcecontrol.SourceControlOperationRunner;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.sourcecontrol.CommitMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class RemoteSourceControlOperationRunner implements SourceControlOperationRunner {

  private static final Gson GSON = new GsonBuilder().create();
  private static final Logger LOG = LoggerFactory.getLogger(SourceControlOperationRunner.class);

  private final RemoteTaskExecutor remoteTaskExecutor;
  private final CConfiguration cConf;

  @Override
  public List<PushAppResponse> push(List<AppDetailsToPush> appsToPush, CommitMeta commitDetails) throws IOException {
    return null;
  }

  @Override
  public PullAppResponse pull(String applicationName, String branchName) throws IOException {
    return null;
  }

  @Override
  public List<ListAppResponse> list() {
    return null;
  }
}
