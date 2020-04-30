/*
 * Copyright © 2018-2020 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.provisioner.existingdataproc;

import com.google.cloud.dataproc.v1.ClusterOperationMetadata;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import io.cdap.cdap.runtime.spi.RuntimeMonitorType;
import io.cdap.cdap.runtime.spi.common.DataprocUtils;
import io.cdap.cdap.runtime.spi.provisioner.Capabilities;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.ClusterStatus;
import io.cdap.cdap.runtime.spi.provisioner.PollingStrategies;
import io.cdap.cdap.runtime.spi.provisioner.PollingStrategy;
import io.cdap.cdap.runtime.spi.provisioner.Provisioner;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerSystemContext;
import io.cdap.cdap.runtime.spi.runtimejob.DataprocClusterInfo;
import io.cdap.cdap.runtime.spi.runtimejob.DataprocRuntimeJobManager;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobDetail;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Connects to existing Dataproc and execute Pipelines. This does not create or teardown Dataproc cluster.
 */
public class ExistingDataprocProvisioner implements Provisioner {

  private static final Logger LOG = LoggerFactory.getLogger(ExistingDataprocProvisioner.class);
  private static final ProvisionerSpecification SPEC = new ProvisionerSpecification(
    "gcp-existing-dataproc", "Existing Dataproc",
    "Connect and Execute jobs on existing Dataproc cluster.");
  private static final String CLUSTER_PREFIX = "cdap-";
  // Keys for looking up system properties
  private static final String LABELS_PROPERTY = "labels";
  private static final String BUCKET = "bucket";
  private static final String RUNTIME_JOB_MANAGER = "runtime.job.manager";
  // keys and values cannot be longer than 63 characters
  // keys and values can only contain lowercase letters, numbers, underscores, and dashes
  // keys must start with a lowercase letter
  // keys cannot be empty
  private static final Pattern LABEL_KEY_PATTERN = Pattern.compile("^[a-z][a-z0-9_-]{0,62}$");
  private static final Pattern LABEL_VAL_PATTERN = Pattern.compile("^[a-z0-9_-]{0,63}$");

  private ProvisionerSystemContext systemContext;

  @Override
  public ProvisionerSpecification getSpec() {
    return SPEC;
  }

  @Override
  public void initialize(ProvisionerSystemContext systemContext) {
    this.systemContext = systemContext;
  }

  /**
   * Parses labels that are expected to be of the form key1=val1,key2=val2 into a map of key values.
   * <p>
   * If a label key or value is invalid, a message will be logged but the key-value will not be returned in the map.
   * Keys and values cannot be longer than 63 characters.
   * Keys and values can only contain lowercase letters, numeric characters, underscores, and dashes.
   * Keys must start with a lowercase letter and must not be empty.
   * <p>
   * If a label is given without a '=', the label value will be empty.
   * If a label is given as 'key=', the label value will be empty.
   * If a label has multiple '=', it will be ignored. For example, 'key=val1=val2' will be ignored.
   *
   * @param labelsStr the labels string to parse
   * @return valid labels from the parsed string
   */
  @VisibleForTesting
  static Map<String, String> parseLabels(String labelsStr) {
    Splitter labelSplitter = Splitter.on(',').trimResults().omitEmptyStrings();
    Splitter kvSplitter = Splitter.on('=').trimResults().omitEmptyStrings();

    Map<String, String> validLabels = new HashMap<>();
    for (String keyvalue : labelSplitter.split(labelsStr)) {
      Iterator<String> iter = kvSplitter.split(keyvalue).iterator();
      if (!iter.hasNext()) {
        continue;
      }
      String key = iter.next();
      String val = iter.hasNext() ? iter.next() : "";
      if (iter.hasNext()) {
        LOG.info("Ignoring invalid label {}. Labels should be of the form 'key=val' or just 'key'", keyvalue);
        continue;
      }
      if (!LABEL_KEY_PATTERN.matcher(key).matches()) {
        LOG.info("Ignoring invalid label key {}. Label keys cannot be longer than 63 characters, must start with "
                   + "a lowercase letter, and can only contain lowercase letters, numeric characters, underscores,"
                   + " and dashes.", key);
        continue;
      }
      if (!LABEL_VAL_PATTERN.matcher(val).matches()) {
        LOG.info("Ignoring invalid label value {}. Label values cannot be longer than 63 characters, "
                   + "and can only contain lowercase letters, numeric characters, underscores, and dashes.", val);
        continue;
      }
      validLabels.put(key, val);
    }
    return validLabels;
  }

  @Override
  public void validateProperties(Map<String, String> properties) {
    systemContext.reloadProperties();
    ExistingDataprocConf conf = ExistingDataprocConf.fromProperties(properties);
    String clusterName = conf.getClusterName();
    //Validate Cluster exists and can connect
    try {
      DataprocClient client = DataprocClient.fromConf(conf);
      if (client.getCluster(clusterName).equals(io.cdap.cdap.runtime.spi.provisioner.ClusterStatus.NOT_EXISTS)) {
        throw new IllegalArgumentException("Cluster " + clusterName + " does not exist."
                                             + " . Validate the cluster exists and " +
                                             "service account has access to the cluster");
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Error Connecting to  Cluster " + clusterName
                                           + " . Validate the cluster exists and " +
                                           "service account has access to the cluster");
    } catch (GeneralSecurityException e) {
      throw new IllegalArgumentException("Security Exception Accessing Cluster " + clusterName +
                                           " . Validate the cluster exists and service " +
                                           "account has access to the cluster");
    } catch (Exception e) {
      throw new IllegalArgumentException("Error Connecting to  Cluster " + clusterName +
                                           " . Validate the cluster exists and service " +
                                           "account has access to the cluster");
    }
  }

  /**
   * This is similar to the Remote Hadoop provisioner.
   * The cluster will be validated again to ensure it still exists before start the pipeline.
   *
   * @param context provisioner context
   * @return
   * @throws Exception
   */
  @Override
  public Cluster createCluster(ProvisionerContext context) throws Exception {
    // Reload system context properties and get system labels
    systemContext.reloadProperties();
    ExistingDataprocConf conf = ExistingDataprocConf.create(createContextProperties(context));

    if (context.getRuntimeMonitorType() == RuntimeMonitorType.SSH && conf.getKeyPair() == null) {
      throw new Exception("SSH User and Key are required for monitoring. Update profile with values.");
    }

    context.getSSHContext().setSSHKeyPair(conf.getKeyPair());
    String clusterName = conf.getClusterName();
    try (DataprocClient client =
           DataprocClient.fromConf(conf)) {
      Optional<Cluster> existing = client.getCluster(clusterName);
      if (existing.isPresent()) {
        //Set CDAP Version label for tracking
        ClusterOperationMetadata createOperationMeta = client.setSystemLabels(getSystemLabels(systemContext),
                                                                              clusterName);
        int numWarnings = createOperationMeta.getWarningsCount();
        if (numWarnings > 0) {
          LOG.warn("Encountered {} warning{} while setting labels on cluster:\n{}",
                   numWarnings, numWarnings > 1 ? "s" : "",
                   String.join("\n", createOperationMeta.getWarningsList()));
        }
      }
      return existing.get();
    }
  }

  @Override
  public ClusterStatus getClusterStatus(ProvisionerContext context, Cluster cluster) throws Exception {
    ClusterStatus status = cluster.getStatus();
    return status == ClusterStatus.DELETING ? ClusterStatus.NOT_EXISTS : status;
  }

  @Override
  public Cluster getClusterDetail(ProvisionerContext context, Cluster cluster) throws Exception {
    return new Cluster(cluster, getClusterStatus(context, cluster));
  }

  @Override
  public void deleteCluster(ProvisionerContext context, Cluster cluster) throws Exception {
    deleteClusterWithStatus(context, cluster);
  }

  @Override
  public ClusterStatus deleteClusterWithStatus(ProvisionerContext context, Cluster cluster) throws Exception {
    // Reload system context properties
    systemContext.reloadProperties();

    Map<String, String> properties = createContextProperties(context);
    ExistingDataprocConf conf = ExistingDataprocConf.fromProperties(properties);
    RuntimeJobManager jobManager = getRuntimeJobManager(context).orElse(null);

    if (jobManager != null) {
      try {
        // If there is job manager, check to make sure the job is completed.
        // Also cleanup files created by the job run.
        RuntimeJobDetail jobDetail = jobManager.getDetail(context.getProgramRunInfo()).orElse(null);
        if (jobDetail != null && !jobDetail.getStatus().isTerminated()) {
          return ClusterStatus.RUNNING;
        }
      } finally {
        jobManager.close();
      }

      Storage storageClient = StorageOptions.newBuilder().setProjectId(conf.getProjectId())
        .setCredentials(conf.getDataprocCredentials()).build().getService();
      DataprocUtils.deleteGCSPath(storageClient, properties.get(BUCKET),
                                  DataprocUtils.CDAP_GCS_ROOT + "/" + context.getProgramRunInfo().getRun());
    }
    return ClusterStatus.DELETING;
  }


  @Override
  public PollingStrategy getPollingStrategy(ProvisionerContext context, Cluster cluster) {
    return PollingStrategies.fixedInterval(0, TimeUnit.SECONDS);
  }

  @Override
  public Capabilities getCapabilities() {
    return new Capabilities(Collections.unmodifiableSet(new HashSet<>(Arrays.asList("fileSet", "externalDataset"))));
  }

  private Map<String, String> getSystemLabels(ProvisionerSystemContext systemContext) {
    Map<String, String> labels = new HashMap<>();
    // dataproc only allows label values to be lowercase letters, numbers, or dashes
    String cdapVersion = systemContext.getCDAPVersion().toLowerCase();
    cdapVersion = cdapVersion.replaceAll("\\.", "_");
    labels.put("cdap-version", cdapVersion);

    String extraLabelsStr = systemContext.getProperties().get(LABELS_PROPERTY);
    // labels are expected to be in format:
    // name1=val1,name2=val2
    if (extraLabelsStr != null) {
      labels.putAll(parseLabels(extraLabelsStr));
    }
    return Collections.unmodifiableMap(labels);
  }

  /**
   * Creates properties for the current context. It will default missing values from the system context properties.
   */
  private Map<String, String> createContextProperties(ProvisionerContext context) {
    Map<String, String> contextProperties = new HashMap<>(context.getProperties());

    // Default the project id from system config if missing or if it is auto-detect
    String contextProjectId = contextProperties.get(ExistingDataprocConf.PROJECT_ID_KEY);
    if (contextProjectId == null || ExistingDataprocConf.AUTO_DETECT.equals(contextProjectId)) {
      contextProjectId = systemContext.getProperties()
        .getOrDefault(ExistingDataprocConf.PROJECT_ID_KEY, contextProjectId);
      if (contextProjectId != null) {
        LOG.trace("Setting default Dataproc project ID to {}", contextProjectId);
        contextProperties.put(ExistingDataprocConf.PROJECT_ID_KEY, contextProjectId);
      }
    }

    // Default settings from the system context
    List<String> keys = new ArrayList<>(
      Arrays.asList(ExistingDataprocConf.RUNTIME_JOB_MANAGER,
                    BUCKET)
    );

    for (String key : keys) {
      if (!contextProperties.containsKey(key)) {
        String value = systemContext.getProperties().get(key);
        if (value != null) {
          LOG.trace("Setting default Dataproc property {} to {}", key, value);
          contextProperties.put(key, value.trim());
        }
      }
    }
    return contextProperties;
  }

  /**
   * Provides implementation of {@link RuntimeJobManager}.
   */
  @Override
  public Optional<RuntimeJobManager> getRuntimeJobManager(ProvisionerContext context) {
    Map<String, String> properties = createContextProperties(context);
    ExistingDataprocConf conf = ExistingDataprocConf.create(properties);
    String clusterName = conf.getClusterName();
    String projectId = conf.getProjectId();
    String region = conf.getRegion();
    String bucket = properties.get(BUCKET);

    Map<String, String> labels = new HashMap<>();
    labels.putAll(getSystemLabels(systemContext));
    //Add User defined Labels to be used for the Jobs
    labels.putAll(conf.getLabels());
    try {
      return Optional.of(
        new DataprocRuntimeJobManager(new DataprocClusterInfo(context, clusterName, conf.getDataprocCredentials(),
                                                              DataprocClient.DATAPROC_GOOGLEAPIS_COM_443,
                                                              projectId, region, bucket, labels)));
    } catch (Exception e) {
      throw new RuntimeException("Error while getting credentials for dataproc. ", e);
    }
  }
}
