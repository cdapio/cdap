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

package co.cask.cdap.etl.realtime.source;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.etl.api.realtime.SourceState;
import co.cask.cdap.etl.common.MockEmitter;
import co.cask.cdap.etl.common.MockRealtimeContext;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import org.elasticmq.Node;
import org.elasticmq.NodeAddress;
import org.elasticmq.NodeBuilder;
import org.elasticmq.rest.RestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.elasticmq.storage.inmemory.InMemoryStorage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

public class SqsSourceTest {
  private Node elasticNode;
  private RestServer sqsRestServer;
  private SQSServer sqsServer;

  @Before
  public void setupElasticMQ() throws Exception {
    elasticNode = NodeBuilder.withStorage(new InMemoryStorage());
    sqsServer = new SQSServer(Networks.getRandomPort());
    NodeAddress nodeAddress = new NodeAddress("http", sqsServer.getHostname(),
                                              sqsServer.getPort(), "");
    SQSRestServerBuilder builder = new SQSRestServerBuilder(elasticNode.nativeClient(), sqsServer.getPort(),
                                                            nodeAddress);
    sqsRestServer = builder.start();
  }

  @Test
  public void testSQS() throws Exception {
    String queueName = "testQueue";
    String testMsgExists = "testMessage1";
    String testMsgOrder = "testMessage2";
    AmazonSQSClient sqsClient = new AmazonSQSClient(new BasicAWSCredentials(SQSServer.AWS_CRED, SQSServer.AWS_CRED));
    sqsClient.setEndpoint(sqsServer.getURL(), "sqs", "");
    sqsClient.createQueue(queueName);
    String queueURL = sqsClient.getQueueUrl(queueName).getQueueUrl();
    sqsClient.sendMessage(queueURL, testMsgExists);
    sqsClient.sendMessage(queueURL, testMsgOrder);

    SqsSource sqsSource = new SqsSource(new SqsSource.SqsConfig("us-west-1", SQSServer.AWS_CRED, SQSServer.AWS_CRED,
                                                                queueName, sqsServer.getURL()));
    sqsSource.initialize(new MockRealtimeContext());

    MockEmitter emitter = new MockEmitter();
    SourceState sourceState = new SourceState();
    sqsSource.poll(emitter, sourceState);

    Assert.assertEquals(2, emitter.getEmitted().size());
    Assert.assertEquals(testMsgExists, ((StructuredRecord) emitter.getEmitted().get(0)).get("body"));
    Assert.assertEquals(testMsgOrder, ((StructuredRecord) emitter.getEmitted().get(1)).get("body"));
  }

  @After
  public void stopElasticMQ() {
    sqsRestServer.stop();
    elasticNode.shutdown();
  }

  private class SQSServer {
    private static final String AWS_CRED = "testKey";
    private final String sqsHostName;
    private final int port;

    public SQSServer(int port) throws Exception {
      this.sqsHostName = InetAddress.getLocalHost().getHostName();
      this.port = port;
    }

    public String getHostname() {
      return sqsHostName;
    }

    public String getURL() {
      return "http://" + sqsHostName + ":" + port;
    }

    public int getPort() {
      return port;
    }
  }
}
