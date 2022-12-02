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

package io.cdap.cdap.app.store;

import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import org.junit.Test;

public class ScanApplicationsRequestTest {
  final NamespaceId nameSpace1 = new NamespaceId("namespace1");
  final NamespaceId nameSpace2 = new NamespaceId("namespace2");
  final ApplicationReference appRef1 = new ApplicationReference(nameSpace1, "app1");
  final ApplicationReference appRef2 = new ApplicationReference(nameSpace2, "app2");
  final ApplicationId appId1 = appRef1.app(ApplicationId.DEFAULT_VERSION);
  final ApplicationId appId2 = appRef2.app(ApplicationId.DEFAULT_VERSION);

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidNamespaceWithScanFrom() {
    ScanApplicationsRequest.builder().setNamespaceId(nameSpace1).setScanFrom(appId2).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidNamespaceWithScanTo() {
    ScanApplicationsRequest.builder().setNamespaceId(nameSpace2).setScanTo(appId1).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidNamespaceWithApplicationName() {
    ScanApplicationsRequest.builder().setApplicationReference(appRef2).setNamespaceId(null).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidAppRefWithScanFrom() {
    ScanApplicationsRequest.builder().setApplicationReference(appRef1).setScanFrom(appId2).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidAppRefWithScanTo() {
    ScanApplicationsRequest.builder().setApplicationReference(appRef2).setScanTo(appId1).build();
  }
}
