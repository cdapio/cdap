/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.common.zookeeper.election;

import co.cask.cdap.common.utils.Tasks;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.internal.zookeeper.DefaultZKClientService;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.internal.zookeeper.LeaderElection;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
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

    ZKClientService infoZKClient = DefaultZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    infoZKClient.startAndWait();
    zkClients.add(infoZKClient);

    // Start the LeaderElectionInfoService
    LeaderElectionInfoService infoService = new LeaderElectionInfoService(infoZKClient, prefix);
    infoService.startAndWait();

    // This will timeout as there is no leader election node created yet
    try {
      infoService.getParticipants(1, TimeUnit.SECONDS);
      Assert.fail("Expected timeout");
    } catch (TimeoutException e) {
      // Expected
    }

    // Starts multiple leader elections
    List<LeaderElection> leaderElections = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      ZKClientService zkClient = DefaultZKClientService.Builder.of(zkServer.getConnectionStr()).build();
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

    // Get the dynamic participants map
    final SortedMap<Integer, LeaderElectionInfoService.Participant> participants =
      infoService.getParticipants(5, TimeUnit.SECONDS);

    // Expects to set all participants with hostname information
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        if (participants.size() != size) {
          return false;
        }
        return Iterables.all(participants.values(), new Predicate<LeaderElectionInfoService.Participant>() {
          @Override
          public boolean apply(LeaderElectionInfoService.Participant input) {
            return input.getHostname() != null;
          }
        });
      }
    }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    // Fetch the static snapshot. It should be the same as the dynamic participants.
    SortedMap<Integer, LeaderElectionInfoService.Participant> snapshot = infoService.fetchCurrentParticipants();
    Assert.assertEquals(size, snapshot.size());
    Assert.assertEquals(participants, snapshot);

    int expectedSize = size;
    for (LeaderElection leaderElection : leaderElections) {
      leaderElection.stopAndWait();
      Tasks.waitFor(--expectedSize, new Callable<Integer>() {
        @Override
        public Integer call() throws Exception {
          return participants.size();
        }
      }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    }

    // Fetch the static snapshot again. It should be empty and the same as the dynamic one.
    snapshot = infoService.fetchCurrentParticipants();
    Assert.assertTrue(snapshot.isEmpty());
    Assert.assertEquals(participants, snapshot);

    infoService.stopAndWait();

    for (ZKClientService zkClient : zkClients) {
      zkClient.stopAndWait();
    }
  }
}
