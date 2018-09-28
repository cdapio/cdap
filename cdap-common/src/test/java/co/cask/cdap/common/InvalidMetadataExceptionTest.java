/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.common;


import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.NamespaceId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link InvalidMetadataException}
 */
public class InvalidMetadataExceptionTest {

  @Test
  public void testMesssages() {
    // test dataset
    InvalidMetadataException invalidMetadataException =
      new InvalidMetadataException(NamespaceId.DEFAULT.dataset("ds").toMetadataEntity(), "error");
    String expectedMessage = "Unable to set metadata for dataset: ds " +
      "which exists in namespace: default. error";
    Assert.assertEquals(expectedMessage, invalidMetadataException.getMessage());

    // test view
    invalidMetadataException =
      new InvalidMetadataException(NamespaceId.DEFAULT.stream("st").view("v").toMetadataEntity(), "error");
    expectedMessage =
      "Unable to set metadata for view: v of stream: st which exists in namespace: default. error";
    Assert.assertEquals(expectedMessage, invalidMetadataException.getMessage());

    // test program
    invalidMetadataException =
      new InvalidMetadataException(NamespaceId.DEFAULT.app("app").program(ProgramType.FLOW, "myflow")
                                     .toMetadataEntity(), "error");
    expectedMessage = "Unable to set metadata for flow: myflow in application: app of version: -SNAPSHOT deployed in " +
      "namespace: default. error";
    Assert.assertEquals(expectedMessage, invalidMetadataException.getMessage());

    // test flowlet
    invalidMetadataException =
      new InvalidMetadataException(NamespaceId.DEFAULT.app("app").program(ProgramType.FLOW, "flow1")
                                     .flowlet("flowlet1").toMetadataEntity(), "error");
    expectedMessage = "Unable to set metadata for flowlet: flowlet1 of flow: flow1 in application: app of " +
      "version: -SNAPSHOT deployed in namespace: default. error";
    Assert.assertEquals(expectedMessage, invalidMetadataException.getMessage());

    // test custom entity
    MetadataEntity customEntity = MetadataEntity.builder(NamespaceId.DEFAULT.dataset("ds").toMetadataEntity())
      .appendAsType("field", "empName").build();
    invalidMetadataException = new InvalidMetadataException(customEntity, "error");
    expectedMessage = "Unable to set metadata for namespace=default,dataset=ds,field=empNam of type 'field'. error";
    Assert.assertEquals(expectedMessage, invalidMetadataException.getMessage());
  }
}
