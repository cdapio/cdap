/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.reactor.client;

import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.reactor.client.common.ClientTestBase;
import co.cask.cdap.test.XSlowTests;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@Category(XSlowTests.class)
public class StreamClientTestRun extends ClientTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(StreamClientTestRun.class);

  private StreamClient streamClient;

  @Before
  public void setUp() throws Throwable {
    ClientConfig config = new ClientConfig("localhost");
    streamClient = new StreamClient(config);
  }

  @Test
  public void testAll() throws Exception {
    String testStreamId = "teststream";
    String testStreamEvent = "blargh_data";

    LOG.info("Getting stream list");
    int baseStreamCount = streamClient.list().size();
    Assert.assertEquals(baseStreamCount, streamClient.list().size());
    LOG.info("Creating stream");
    streamClient.create(testStreamId);
    LOG.info("Checking stream list");
    Assert.assertEquals(baseStreamCount + 1, streamClient.list().size());
    // TODO: getting and setting config for stream is not supported with in-memory
//    streamClient.setTTL(testStreamId, 123);
//    streamClient.sendEvent(testStreamId, testStreamEvent);
//    streamClient.truncate(testStreamId);
//    streamClient.sendEvent(testStreamId, testStreamEvent);
//    String consumerId = streamClient.getConsumerId(testStreamId);
//    Assert.assertEquals(testStreamEvent, streamClient.dequeueEvent(testStreamId, consumerId));
//    Assert.assertEquals(null, streamClient.dequeueEvent(testStreamId, consumerId));
  }
}
