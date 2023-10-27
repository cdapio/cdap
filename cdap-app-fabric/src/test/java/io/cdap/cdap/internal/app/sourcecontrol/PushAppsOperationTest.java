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

package io.cdap.cdap.internal.app.sourcecontrol;


import com.google.gson.Gson;
import io.cdap.cdap.internal.operation.LongRunningOperationContext;
import io.cdap.cdap.proto.operation.OperationResource;
import io.cdap.cdap.sourcecontrol.operationrunner.InMemorySourceControlOperationRunner;
import java.util.Set;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

public class PushAppsOperationTest {
  private static final Gson GSON = new Gson();
  private InMemorySourceControlOperationRunner opRunner;
  private LongRunningOperationContext context;
  private Set<OperationResource> gotResources;

  @ClassRule
  public static TemporaryFolder repositoryBase = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    // TODO setup tests
  }
}
