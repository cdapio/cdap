/*
 * Copyright © 2017-2024 Cask Data, Inc.
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

package io.cdap.cdap.common.zookeeper.election;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import io.cdap.cdap.common.utils.Tasks;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.internal.zookeeper.DefaultZKClientService;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.internal.zookeeper.LeaderElection;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for {@link LeaderElectionInfoService}.
 */
public class LeaderElectionInfoServiceTest {

  private static final Logger LOG = LoggerFactory.getLogger(LeaderElectionInfoServiceTest.class);

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static InMemoryZKServer zkServer;

  @BeforeClass
  public static void init() throws IOException {
    zkServer = InMemoryZKServer.builder().setDataDir(TEMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();
  }

  @AfterClass
  public static void finish() {
    zkServer.stopAndWait();
  }

  @Test
  public void testParticipants() throws Exception {
    final int size = 5;
    String prefix = "/election";

    List<ZKClientService> zkClients = new ArrayList<>();

    ZKClientService infoZkClient =
        DefaultZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    infoZkClient.startAndWait();
    zkClients.add(infoZkClient);

    LeaderElectionInfoService infoService = new LeaderElectionInfoService(infoZkClient, prefix);
    infoService.startAndWait();

    try {
      infoService.getParticipants(200, TimeUnit.MILLISECONDS);
      Assert.fail("Expected timeout");
    } catch (TimeoutException expected) {
      // expected
    }

    List<LeaderElection> leaderElections = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      ZKClientService zkClient =
          DefaultZKClientService.Builder.of(zkServer.getConnectionStr()).build();
      zkClient.startAndWait();
      zkClients.add(zkClient);

      final int participantId = i;
      LeaderElection leaderElection = new LeaderElection(zkClient, prefix, new ElectionHandler() {
        @Override
        public void leader() {
          LOG.info("Leader: {}", participantId);
        }

        @Override
        public void follower() {
          LOG.info("Follow: {}", participantId);
        }
      });
      leaderElection.start();
      leaderElections.add(leaderElection);
    }

    final SortedMap<Integer, LeaderElectionInfoService.Participant> participants =
        infoService.getParticipants(5, TimeUnit.SECONDS);

    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() {
        if (participants.size() != size) {
          return false;
        }
        return Iterables.all(participants.values(),
            new Predicate<LeaderElectionInfoService.Participant>() {
              @Override
              public boolean apply(LeaderElectionInfoService.Participant input) {
                return input.getHostname() != null;
              }
            });
      }
    }, 5, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);

    SortedMap<Integer, LeaderElectionInfoService.Participant> snapshot =
        infoService.fetchCurrentParticipants();
    Assert.assertEquals(size, snapshot.size());
    Assert.assertEquals(participants, snapshot);

    int expectedSize = size;
    for (LeaderElection leaderElection : leaderElections) {
      leaderElection.stopAndWait();
      Tasks.waitFor(--expectedSize, new Callable<Integer>() {
        @Override
        public Integer call() {
          return participants.size();
        }
      }, 5, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
    }

    snapshot = infoService.fetchCurrentParticipants();
    Assert.assertTrue(snapshot.isEmpty());
    Assert.assertEquals(participants, snapshot);

    infoService.stopAndWait();

    for (ZKClientService zkClient : zkClients) {
      zkClient.stopAndWait();
    }
  }

  @Test
  public void testNonSlashedPathAndParticipantMetadataCallbacks() throws Exception {
    ZKClientService zkClient =
        DefaultZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();
    try {
      String relativePath = "customElectionPath";
      zkClient.create("/" + relativePath + "/invalidNoDash", null,
          CreateMode.PERSISTENT, true).get(5, TimeUnit.SECONDS);
      zkClient.create("/" + relativePath + "/guid-notAnInt", null,
          CreateMode.PERSISTENT, true).get(5, TimeUnit.SECONDS);
      zkClient.create("/" + relativePath + "/guid-0000000001",
          "host-a.local".getBytes(StandardCharsets.UTF_8),
          CreateMode.PERSISTENT, true).get(5, TimeUnit.SECONDS);

      LeaderElectionInfoService infoService =
          new LeaderElectionInfoService(zkClient, relativePath);
      infoService.startAndWait();
      try {
        SortedMap<Integer, LeaderElectionInfoService.Participant> map =
            infoService.getParticipants(5, TimeUnit.SECONDS);
        Tasks.waitFor("host-a.local", () -> {
          LeaderElectionInfoService.Participant p = map.get(1);
          return p == null ? null : p.getHostname();
        }, 5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);

        LeaderElectionInfoService.Participant participant = map.get(1);
        Assert.assertEquals("/customElectionPath/guid-0000000001", participant.getZkPath());
        Assert.assertEquals("host-a.local", participant.getHostname());
        Assert.assertTrue(participant.toString().contains("host-a.local"));
      } finally {
        infoService.stopAndWait();
      }
    } finally {
      zkClient.stopAndWait();
    }
  }
}
