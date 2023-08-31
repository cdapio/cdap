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

package io.cdap.cdap.storage.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.Op;
import com.google.cloud.spanner.Session;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TestInsert {
  public static void main(String[] args) {
    int payload = 1024 * 100;
    int iteration = 2;
    int batch = 500;
    byte[] arr = new byte[payload];

    // Create a Spanner client
    Spanner spanner =
        SpannerOptions.newBuilder().setProjectId("ardekani-cdf-sandbox2").build().getService();
    try {

      DatabaseId db = DatabaseId.of("ardekani-cdf-sandbox2", "latest-test", "test-db");
      // Connect to the database
      DatabaseClient dbClient = spanner.getDatabaseClient(db);
      DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();
      InstanceAdminClient instanceAdminClient = spanner.getInstanceAdminClient();

      ByteArray ba = ByteArray.copyFrom(arr);
      List<Mutation> mutations = new ArrayList<>();
      long start = System.currentTimeMillis();
      for (int i = 0; i < iteration; i++) {
        mutations.clear();
        for (int j = 0; j < batch; j++) {
          Mutation mutation =
              Mutation.newInsertBuilder("topic_name")
                  .set("SequenceNo")
                  .to(batch-j)
                  .set("MessageChunk")
                  .to(0)
                  .set("CommitTs")
                  .to("spanner.commit_timestamp()")
                  .set("Message")
                  .to(ba)
                  .build();
          mutations.add(mutation);
        }
        dbClient.write(mutations);
      }
      long duration = System.currentTimeMillis() - start;
      String msg =
          String.format(
              "For %s iterations of batch size %s {total %s insertions}, it took %s ms with each"
                  + " message size %s bytes",
              iteration, batch, iteration * batch, duration, payload);
      System.out.println(msg);
      // writeUsingDml(dbClient);
    } finally {
      // Close the session
      spanner.close();
    }
  }

  static void writeUsingDml(DatabaseClient dbClient) {

    //
    // byte[] arr = new byte[1000000];
    // // Insert 4 singer records
    // dbClient
    //     .readWriteTransaction()
    //     .run(
    //         transaction -> {
    //           String sql =
    //               "INSERT INTO topic_name (SequenceNo, MessageChunk, CommitTs, Message) VALUES "
    //                   + "(1,0, PENDING_COMMIT_TIMESTAMP(),"
    //                   + arr
    //                   + ")";
    //           long rowCount = transaction.executeUpdate(Statement.of(sql));
    //           System.out.printf("%d records inserted.\n", rowCount);
    //           return null;
    //         });
  }
}
