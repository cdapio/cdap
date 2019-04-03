/*
 * Copyright © 2015-2019 Cask Data, Inc.
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
package io.cdap.cdap.common;


import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
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

    // test program
    invalidMetadataException =
      new InvalidMetadataException(NamespaceId.DEFAULT.app("app").program(ProgramType.WORKER, "wk")
                                     .toMetadataEntity(), "error");
    expectedMessage = "Unable to set metadata for worker: wk in application: app of version: -SNAPSHOT deployed in " +
      "namespace: default. error";
    Assert.assertEquals(expectedMessage, invalidMetadataException.getMessage());

    // test custom entity
    MetadataEntity customEntity = MetadataEntity.builder(NamespaceId.DEFAULT.dataset("ds").toMetadataEntity())
      .appendAsType("field", "empName").build();
    invalidMetadataException = new InvalidMetadataException(customEntity, "error");
    expectedMessage = "Unable to set metadata for namespace=default,dataset=ds,field=empName of type 'field'. error";
    Assert.assertEquals(expectedMessage, invalidMetadataException.getMessage());
  }
}
