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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Properties;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.WorkflowManager;
import com.dumbster.smtp.SimpleSmtpServer;
import com.dumbster.smtp.SmtpMessage;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link EmailAction}
 */
public class ETLEmailActionTest extends BaseETLBatchTest {

  private SimpleSmtpServer server;
  private int port;

  @Before
  public void beforeTest() {
    port = Networks.getRandomPort();
    server = SimpleSmtpServer.start(port);
  }

  @Test
  public void testEmailAction() throws Exception {
    // kv table to kv table pipeline
    ETLStage source = new ETLStage("KVTable", ImmutableMap.of(Properties.BatchReadableWritable.NAME, "table1"));
    ETLStage sink = new ETLStage("KVTable", ImmutableMap.of(Properties.BatchReadableWritable.NAME, "table2"));
    ETLStage transform = new ETLStage("Projection", ImmutableMap.<String, String>of());
    List<ETLStage> transformList = Lists.newArrayList(transform);
    ETLStage action = new ETLStage("Email", ImmutableMap.of(EmailAction.RECIPIENT_EMAIL_ADDRESS, "to@test.com",
                                                            EmailAction.FROM_ADDRESS, "from@test.com",
                                                            EmailAction.MESSAGE, "testing body",
                                                            EmailAction.SUBJECT, "Test",
                                                            EmailAction.PORT, Integer.toString(port)));
    List<ETLStage> actionList = Lists.newArrayList(action);
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transformList, actionList);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "actionTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // add some data to the input table
    DataSetManager<KeyValueTable> table1 = getDataset("table1");
    KeyValueTable inputTable = table1.get();
    inputTable.write("hello", "world");
    table1.flush();

    WorkflowManager manager = appManager.getWorkflowManager(ETLWorkflow.NAME);
    manager.start();
    manager.waitForFinish(5, TimeUnit.MINUTES);

    server.stop();

    Assert.assertEquals(1, server.getReceivedEmailSize());
    Iterator emailIter = server.getReceivedEmail();
    SmtpMessage email = (SmtpMessage) emailIter.next();
    Assert.assertEquals("Test", email.getHeaderValue("Subject"));
    Assert.assertTrue(email.getBody().startsWith("testing body"));
    Assert.assertFalse(emailIter.hasNext());
  }
}
