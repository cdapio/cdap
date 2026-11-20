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

package io.cdap.cdap.data2.audit;

import com.google.gson.Gson;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.spi.StoreRequest;
import io.cdap.cdap.proto.audit.AuditMessage;
import io.cdap.cdap.proto.audit.AuditPayload;
import io.cdap.cdap.proto.audit.AuditType;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DefaultAuditPublisher}.
 */
public class DefaultAuditPublisherTest {

  private static final Gson GSON = new Gson();

  @Test
  public void testPublishUsesEmptyStringWhenUserIdIsNullAndActualUserWhenSet() throws Exception {
    final List<String> payloads = new ArrayList<>();
    MessagingService messagingService = Mockito.mock(MessagingService.class);
    Mockito.when(messagingService.publish(Mockito.any(StoreRequest.class))).thenAnswer(inv -> {
      StoreRequest req = inv.getArgument(0);
      for (byte[] bytes : req) {
        payloads.add(Bytes.toString(bytes));
      }
      return null;
    });

    CConfiguration cConf = CConfiguration.create();
    DefaultAuditPublisher publisher = new DefaultAuditPublisher(cConf, messagingService);
    DatasetId datasetId = NamespaceId.DEFAULT.dataset("auditDs");

    String previousUser = SecurityRequestContext.getUserId();
    try {
      SecurityRequestContext.setUserId(null);
      publisher.publish(datasetId, AuditType.CREATE, AuditPayload.EMPTY_PAYLOAD);

      SecurityRequestContext.setUserId("bob");
      publisher.publish(datasetId, AuditType.UPDATE, AuditPayload.EMPTY_PAYLOAD);
    } finally {
      SecurityRequestContext.setUserId(previousUser);
    }

    Assert.assertEquals(2, payloads.size());
    AuditMessage msgWithNullUser = GSON.fromJson(payloads.get(0), AuditMessage.class);
    Assert.assertEquals("", msgWithNullUser.getUser());
    Assert.assertEquals(AuditType.CREATE, msgWithNullUser.getType());

    AuditMessage msgWithBob = GSON.fromJson(payloads.get(1), AuditMessage.class);
    Assert.assertEquals("bob", msgWithBob.getUser());
    Assert.assertEquals(AuditType.UPDATE, msgWithBob.getType());
  }
}
