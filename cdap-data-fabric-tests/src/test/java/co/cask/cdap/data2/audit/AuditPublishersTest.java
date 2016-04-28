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

package co.cask.cdap.data2.audit;

import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.audit.AuditMessage;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Unit test for {@link AuditPublishers}.
 */
public class AuditPublishersTest {

  @Test
  public void testPublishingAccessLogs() {
    String datasetName = "dummyDataset";
    String datasetName2 = "dummyDataset2";
    String appName = "dummyApp";
    String workerName = "dummyWorker";
    String workerName2 = "dummyWorker2";
    InMemoryAuditPublisher auditPublisher = new InMemoryAuditPublisher();
    Id.Worker workerId = Id.Worker.from(Id.Namespace.DEFAULT, appName, workerName);
    Id.DatasetInstance datasetId = Id.DatasetInstance.from(Id.Namespace.DEFAULT, datasetName);
    AuditPublishers.publishAccess(auditPublisher, datasetId, AccessType.READ_WRITE, workerId);
    List<AuditMessage> messages = auditPublisher.popMessages();
    // Since it is a READ_WRITE access, two messages are expected
    Assert.assertEquals(2, messages.size());

    // Same access so no message should be published
    AuditPublishers.publishAccess(auditPublisher, datasetId, AccessType.READ_WRITE, workerId);
    messages = auditPublisher.popMessages();
    Assert.assertEquals(0, messages.size());

    // Different accesstype, hence a message should be published
    AuditPublishers.publishAccess(auditPublisher, datasetId, AccessType.READ, workerId);
    messages = auditPublisher.popMessages();
    Assert.assertEquals(1, messages.size());

    // Different dataset name, hence a message should be published
    datasetId = Id.DatasetInstance.from(Id.Namespace.DEFAULT, datasetName2);
    AuditPublishers.publishAccess(auditPublisher, datasetId, AccessType.READ_WRITE, workerId);
    messages = auditPublisher.popMessages();
    Assert.assertEquals(2, messages.size());

    // Different worker name, hence a message should be published
    workerId = Id.Worker.from(Id.Namespace.DEFAULT, appName, workerName2);
    AuditPublishers.publishAccess(auditPublisher, datasetId, AccessType.READ_WRITE, workerId);
    messages = auditPublisher.popMessages();
    Assert.assertEquals(2, messages.size());
  }
}
