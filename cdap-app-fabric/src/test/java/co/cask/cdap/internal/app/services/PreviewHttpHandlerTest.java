/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.internal.app.services;

import co.cask.cdap.gateway.handlers.preview.PreviewHttpHandler;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.id.ApplicationId;
import org.junit.Test;

import java.util.HashSet;

/**
 * Test for {@link PreviewHttpHandler}.
 */
public class PreviewHttpHandlerTest extends AppFabricTestBase {

  @Test
  public void testInjector() throws Exception {
    PreviewHttpHandler handler = getInjector().getInstance(PreviewHttpHandler.class);
    handler.createPreviewInjector(new ApplicationId("ns", "app"), new HashSet<String>());
  }
}
