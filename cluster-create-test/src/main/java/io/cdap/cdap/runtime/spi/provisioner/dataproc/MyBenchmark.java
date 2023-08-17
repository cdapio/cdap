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

package io.cdap.cdap.runtime.spi.provisioner.dataproc;

import com.google.cloud.dataproc.v1.ClusterOperationMetadata;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.runtime.spi.provisioner.ClusterStatus;
import io.cdap.cdap.runtime.spi.provisioner.RetryableProvisionException;
import io.cdap.cdap.runtime.spi.ssh.SSHContext;
import io.cdap.cdap.runtime.spi.ssh.SSHKeyPair;
import io.cdap.cdap.runtime.spi.ssh.SSHPublicKey;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Threads(5) @Warmup(iterations = 0)
public class MyBenchmark {

    private static AtomicLong clusterNum = new AtomicLong(System.currentTimeMillis());

    private static final Logger LOG = LoggerFactory.getLogger(MyBenchmark.class);
    @State(Scope.Thread)
    public static class BenchmarkState {

        public String clusterName;

        public SSHPublicKey sshPublicKey;

        public DataprocClient dataprocClient;

        public ClusterStatus clusterStatus;

        @Setup(Level.Invocation)
        public void setUp() throws GeneralSecurityException, IOException {
            SSHContext sshContext = new DefaultSSHContext(null, null, null);
            SSHKeyPair sshKeyPair = sshContext.getSSHKeyPair().orElse(null);
            if (sshKeyPair == null) {
                sshKeyPair = sshContext.generate("cdap");
                sshContext.setSSHKeyPair(sshKeyPair);
            }
            sshPublicKey = sshKeyPair.getPublicKey();

            DataprocClientFactory clientFactory = new DefaultDataprocClientFactory();
            DataprocConf conf = DataprocConf.create(ImmutableMap.<String, String>builder()
                .put(DataprocConf.PROJECT_ID_KEY, "cask-audi-clusters")
                .put("region", "us-central1")
                .put("network", "network")

                .put("masterNumNodes", "1")
                .put("masterCPUs", "2")
                .put("masterMemoryMB", "8192")
                .put("masterDiskGB", "1000")
                .put("workerNumNodes", "3")
                .put("workerCPUs", "4")
                .put("workerMemoryMB", "15360")
                .put("workerDiskGB", "500")
                .put("preferExternalIP", "true")
                .put("stackdriverLoggingEnabled", "true")
                .put("stackdriverMonitoringEnabled", "true")
                .put("idleTTL", "1200")
                .build()
            );
            dataprocClient = clientFactory.create(conf, true);

        }

        @TearDown(Level.Invocation)
        public void tearDown() throws RetryableProvisionException, InterruptedException {
            if (clusterStatus != ClusterStatus.RUNNING) {
                LOG.error("Skipping cluster delete as cluster was not running");
            } else {
                LOG.warn("Deleting cluster {}", clusterName);
                dataprocClient.deleteCluster(clusterName);
                LOG.warn("Deleted cluster {}", clusterName);
            }
            dataprocClient.close();
        }
    }

    @Benchmark @BenchmarkMode(Mode.SingleShotTime)
    public void test20(BenchmarkState state)
        throws RetryableProvisionException, GeneralSecurityException, IOException, InterruptedException {
        testMethod("2.0", state);
    }
    @Benchmark @BenchmarkMode(Mode.SingleShotTime)
    public void test2068(BenchmarkState state)
        throws RetryableProvisionException, GeneralSecurityException, IOException, InterruptedException {
        testMethod("2.0.68-debian10", state);
    }

    public void testMethod(String version, BenchmarkState state)
        throws GeneralSecurityException, IOException, InterruptedException, RetryableProvisionException {
        state.clusterName = "test-" + version.replaceAll("[^a-zA-Z0-9]+", "-") + "-" + clusterNum.incrementAndGet();
        LOG.warn("Creating cluster {}", state.clusterName);
        ClusterOperationMetadata cluster = state.dataprocClient.createCluster(
            state.clusterName,
            version, ImmutableMap.of(), false, state.sshPublicKey);
        state.clusterStatus = ClusterStatus.CREATING;
        for (int i = 0; state.clusterStatus == ClusterStatus.CREATING && i < 420; i++) {
            state.clusterStatus = state.dataprocClient.getClusterStatus(state.clusterName);
            Thread.sleep(1000);
        }
        if (state.clusterStatus != ClusterStatus.RUNNING) {
            throw new IllegalStateException("Cluster " + state.clusterName + " is in state " + state.clusterStatus);
        }
    }
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(MyBenchmark.class.getSimpleName())
            .measurementIterations(2)
            .syncIterations(false)
            .build();

        new Runner(opt).run();
    }
}
