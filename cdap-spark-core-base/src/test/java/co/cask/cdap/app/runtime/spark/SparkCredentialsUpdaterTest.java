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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.io.Locations;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class SparkCredentialsUpdaterTest {

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Test
  public void testUpdater() throws Exception {
    Location credentialsDir = Locations.toLocation(TEMPORARY_FOLDER.newFolder());

    // Create a updater that don't do any auto-update within the test time and don't cleanup
    SparkCredentialsUpdater updater = new SparkCredentialsUpdater(createCredentialsSupplier(),
                                                                  credentialsDir, "credentials",
                                                                  TimeUnit.DAYS.toMillis(1),
                                                                  TimeUnit.DAYS.toMillis(1), Integer.MAX_VALUE) {
      @Override
      long getNextUpdateDelay(Credentials credentials) throws IOException {
        return TimeUnit.DAYS.toMillis(1);
      }
    };

    // Before the updater starts, the directory is empty
    Assert.assertTrue(credentialsDir.list().isEmpty());

    UserGroupInformation.getCurrentUser().addToken(new Token<>(Bytes.toBytes("id"), Bytes.toBytes("pass"),
                                                               new Text("kind"), new Text("service")));

    updater.startAndWait();
    try {
      List<Location> expectedFiles = new ArrayList<>();
      expectedFiles.add(credentialsDir.append("credentials-1"));

      for (int i = 1; i <= 10; i++) {
        Assert.assertEquals(expectedFiles, listAndSort(credentialsDir));

        // Read the credentials from the last file
        Credentials newCredentials = new Credentials();
        try (DataInputStream is = new DataInputStream(expectedFiles.get(expectedFiles.size() - 1).getInputStream())) {
          newCredentials.readTokenStorageStream(is);
        }

        // Should contains all tokens of the current user
        Credentials userCredentials = UserGroupInformation.getCurrentUser().getCredentials();
        for (Token<? extends TokenIdentifier> token : userCredentials.getAllTokens()) {
          Assert.assertEquals(token, newCredentials.getToken(token.getService()));
        }

        UserGroupInformation.getCurrentUser().addToken(new Token<>(Bytes.toBytes("id" + i), Bytes.toBytes("pass" + i),
                                                                   new Text("kind" + i), new Text("service" + i)));
        updater.run();
        expectedFiles.add(credentialsDir.append("credentials-" + (i + 1)));
      }
    } finally {
      updater.stopAndWait();
    }
  }

  @Test
  public void testCleanup() throws IOException, InterruptedException {
    Location credentialsDir = Locations.toLocation(TEMPORARY_FOLDER.newFolder());

    // Create a updater that don't do any auto-update within the test time
    SparkCredentialsUpdater updater = new SparkCredentialsUpdater(createCredentialsSupplier(),
                                                                  credentialsDir, "credentials",
                                                                  TimeUnit.DAYS.toMillis(1),
                                                                  TimeUnit.SECONDS.toMillis(3), 3) {
      @Override
      long getNextUpdateDelay(Credentials credentials) throws IOException {
        return TimeUnit.DAYS.toMillis(1);
      }
    };

    updater.startAndWait();
    try {
      // Expect this loop to finish in 3 seconds because we don't want sleep for too long for testing cleanup
      for (int i = 1; i <= 5; i++) {
        Assert.assertEquals(i, credentialsDir.list().size());
        updater.run();
      }

      // Sleep for 3 seconds, which is the cleanup expire time
      TimeUnit.SECONDS.sleep(3);

      // Run the updater again, it should only have three files (2 older than expire time, 1 new)
      updater.run();
      Assert.assertEquals(3, credentialsDir.list().size());
    } finally {
      updater.stopAndWait();
    }
  }

  /**
   * List all files under the given directory in ascending order of the suffix number.
   */
  private List<Location> listAndSort(Location dir) throws IOException {
    final Pattern pattern = Pattern.compile("-(\\d+)$");

    List<Location> locations = new ArrayList<>(dir.list());
    Collections.sort(locations, new Comparator<Location>() {
      @Override
      public int compare(Location o1, Location o2) {
        try {
          Matcher matcher1 = pattern.matcher(o1.getName());
          Matcher matcher2 = pattern.matcher(o2.getName());

          if (matcher1.find() && matcher2.find()) {
            return Long.compare(Long.parseLong(matcher1.group(1)), Long.parseLong(matcher2.group(1)));
          }
          throw new IllegalArgumentException("Expect name ended with number suffix");
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    });
    return locations;
  }

  private Supplier<Credentials> createCredentialsSupplier() {
    return new Supplier<Credentials>() {
      @Override
      public Credentials get() {
        try {
          return UserGroupInformation.getCurrentUser().getCredentials();
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }
}
