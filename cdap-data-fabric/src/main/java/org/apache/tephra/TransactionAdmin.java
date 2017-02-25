/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.runtime.ConfigModule;
import org.apache.tephra.runtime.DiscoveryModules;
import org.apache.tephra.runtime.TransactionClientModule;
import org.apache.tephra.runtime.TransactionModules;
import org.apache.tephra.runtime.ZKModule;
import org.apache.twill.zookeeper.ZKClientService;

import java.io.PrintStream;
import java.util.List;
import java.util.Set;

/**
 * Allows calling some methods on {@link TransactionManager} from command line.
 */
public class TransactionAdmin {
  private static final String OPT_TRUNCATE_INVALID_TX = "--truncate-invalid-tx";
  private static final String OPT_TRUNCATE_INVALID_TX_BEFORE = "--truncate-invalid-tx-before";
  private static final String OPT_GET_INVALID_TX_SIZE = "--get-invalid-tx-size";
  
  private final PrintStream out;
  private final PrintStream err;

  public static void main(String[] args) {
    TransactionAdmin txAdmin = new TransactionAdmin(System.out, System.err);
    int status = txAdmin.doMain(args, new Configuration());
    System.exit(status);
  }

  public TransactionAdmin(PrintStream out, PrintStream err) {
    this.out = out;
    this.err = err;
  }

  @VisibleForTesting
  int doMain(String[] args, Configuration conf) {
    if (args.length < 1) {
      printUsage();
      return 1;
    }

    Injector injector = Guice.createInjector(
      new ConfigModule(conf),
      new ZKModule(),
      new DiscoveryModules().getDistributedModules(),
      new TransactionModules().getDistributedModules(),
      new TransactionClientModule()
    );

    ZKClientService zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAndWait();
    
    try {
      TransactionSystemClient txClient = injector.getInstance(TransactionSystemClient.class);
      String option = args[0];
      
      if (option.equals(OPT_TRUNCATE_INVALID_TX)) {
        if (args.length != 2) {
          printUsage();
          return 1;
        }
        Set<Long> txIds;
        try {
          txIds = parseTxIds(args[1]);
        } catch (NumberFormatException e) {
          err.println("NumberFormatException: " + e.getMessage());
          return 1;
        }
        if (!txIds.isEmpty()) {
          out.println("Invalid list size before truncation: " + txClient.getInvalidSize());
          txClient.truncateInvalidTx(txIds);
          out.println("Invalid list size after truncation: " + txClient.getInvalidSize());
        }
      } else if (option.equals(OPT_TRUNCATE_INVALID_TX_BEFORE)) {
        if (args.length != 2) {
          printUsage();
          return 1;
        }
        try {
          long time = Long.parseLong(args[1]);
          out.println("Invalid list size before truncation: " + txClient.getInvalidSize());
          txClient.truncateInvalidTxBefore(time);
          out.println("Invalid list size after truncation: " + txClient.getInvalidSize());
        } catch (InvalidTruncateTimeException e) {
          err.println(e.getMessage());
          return 1;
        } catch (NumberFormatException e) {
          err.println("NumberFormatException: " + e.getMessage());
          return 1;
        }
      } else if (option.equals(OPT_GET_INVALID_TX_SIZE)) {
        if (args.length != 1) {
          printUsage();
          return 1;
        }
        out.println("Invalid list size: " + txClient.getInvalidSize());
      } else {
        printUsage();
        return 1;
      }
    } finally {
      zkClient.stopAndWait();
    }
    return 0;
  }
  
  private Set<Long> parseTxIds(String option) throws NumberFormatException {
    Set<Long> txIds = Sets.newHashSet();
    for (String str : Splitter.on(',').split(option)) {
      txIds.add(Long.parseLong(str));
    }
    return txIds;
  }
  
  private void printUsage() {
    String programName = TransactionAdmin.class.getSimpleName();
    String spaces = "     ";
    List<String> options = Lists.newArrayList();
    options.add(join("Usage: "));
    options.add(join(spaces, programName, OPT_TRUNCATE_INVALID_TX, "<tx1,tx2,...>"));
    options.add(join(spaces, programName, OPT_TRUNCATE_INVALID_TX_BEFORE, "<time in secs>"));
    options.add(join(spaces, programName, OPT_GET_INVALID_TX_SIZE));
    
    String usage = Joiner.on(System.getProperty("line.separator")).join(options);
    err.println(usage);
  }
  
  private static String join(String... args) {
    return Joiner.on(" ").join(args);
  }
}
