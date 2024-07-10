/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.cloud.dataproc.v1.ClusterControllerSettings;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.runtime.spi.common.DataprocImageVersion;
import io.cdap.cdap.runtime.spi.common.DataprocUtils;
import io.cdap.cdap.runtime.spi.provisioner.Capabilities;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.ClusterStatus;
import io.cdap.cdap.runtime.spi.provisioner.Provisioner;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerSystemContext;
import io.cdap.cdap.runtime.spi.runtimejob.DataprocClusterInfo;
import io.cdap.cdap.runtime.spi.runtimejob.DataprocRuntimeJobDetail;
import io.cdap.cdap.runtime.spi.runtimejob.DataprocRuntimeJobManager;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobDetail;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobManager;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobStatus;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract implementation for Dataproc based {@link Provisioner}.
 */
public abstract class AbstractDataprocProvisioner implements Provisioner {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractDataprocProvisioner.class);
  // Keys for looking up system properties
  private static final String LABELS_PROPERTY = "labels";
  private static final Pattern SIMPLE_VERSION_PATTERN = Pattern.compile("^([0-9][0-9.]*)$");
  private static final Pattern CLUSTER_VERSION_PATTERN = Pattern.compile("^([0-9][0-9.]*)-.*");
  protected static final DataprocImageVersion DATAPROC_1_5_VERSION = new DataprocImageVersion(
      "1.5");
  public static final String LABEL_VERSON = "cdap-version";
  public static final String LABEL_PROFILE = "cdap-profile";
  public static final String LABEL_REUSE_KEY = "cdap-reuse-key";
  public static final String LABEL_REUSE_UNTIL = "cdap-reuse-until";
  /**
   * In reuse scenario we can't find "our" cluster by cluster name, so let's put it into the label
   *
   * @see {@link DataprocProvisioner#getAllocatedClusterName(ProvisionerContext)}
   */
  public static final String LABEL_RUN_KEY = "cdap-run-key";

  private final ProvisionerSpecification spec;
  private ProvisionerSystemContext systemContext;

  protected AbstractDataprocProvisioner(ProvisionerSpecification spec) {
    this.spec = spec;
  }

  @Override
  public final ProvisionerSpecification getSpec() {
    return spec;
  }

  @Override
  public final void initialize(ProvisionerSystemContext systemContext) {
    this.systemContext = systemContext;

    // invalidate twill and launcher jar cache.
    if (Files.exists(DataprocUtils.CACHE_DIR_PATH)) {
      DataprocUtils.deleteDirectoryWithRetries(DataprocUtils.CACHE_DIR_PATH.toFile(),
          "Unable to delete local cache directory %s for "
              + "twill.jar and launcher.jar");
    }
  }

  @Override
  public final void deleteCluster(ProvisionerContext context, Cluster cluster) throws Exception {
    deleteClusterWithStatus(context, cluster);
  }

  @Override
  public final ClusterStatus deleteClusterWithStatus(ProvisionerContext context, Cluster cluster)
      throws Exception {
    Map<String, String> properties = createContextProperties(context);
    DataprocConf conf = DataprocConf.create(properties);
    RuntimeJobManager jobManager = getRuntimeJobManager(context).orElse(null);

    // If there is job manager, check to make sure the job is completed.
    // Also cleanup files created by the job run.
    if (jobManager != null) {
      try {
        RuntimeJobDetail jobDetail = jobManager.getDetail(context.getProgramRunInfo()).orElse(null);
        if (jobDetail != null && !jobDetail.getStatus().isTerminated()) {
          return ClusterStatus.RUNNING;
        }

        if (jobDetail != null
            && jobDetail.getStatus() != RuntimeJobStatus.COMPLETED
            && (jobDetail instanceof DataprocRuntimeJobDetail)) {
          // Status details is specific to dataproc jobs, so it was not added to RuntimeJobDetail spi.
          String statusDetails = ((DataprocRuntimeJobDetail) jobDetail).getJobStatusDetails();
          if (statusDetails != null) {
            LOG.error("Dataproc job '{}' with the status details: {}",
                jobDetail.getStatus().name(), statusDetails);
          }
        }
      } finally {
        jobManager.close();
      }

      // Due to historical reasons bucket is used if conf.getGcsBucket is not provided
      String bucket =
          conf.getGcsBucket() != null ? conf.getGcsBucket() : properties.get(DataprocUtils.BUCKET);
      Storage storageClient = StorageOptions.newBuilder().setProjectId(conf.getProjectId())
          .setCredentials(conf.getDataprocCredentials()).build().getService();
      String runId = context.getProgramRunInfo().getRun();
      String runRootPath = getPath(DataprocUtils.CDAP_GCS_ROOT, runId);
      DataprocUtils.deleteGcsPath(storageClient, bucket, runRootPath);
    }

    doDeleteCluster(context, cluster, conf);
    return ClusterStatus.DELETING;
  }

  private String getPath(String... pathSubComponents) {
    return Joiner.on("/").join(pathSubComponents);
  }

  /**
   * Gets the default cluster name for the given context. See {@link DataprocProvisioner#getClusterName} to
   * get the name of actually allocated cluster (if any).
   *
   * @param context the context
   * @return a string that is a valid cluster name
   */
  protected abstract String getClusterName(ProvisionerContext context) throws Exception;

  /**
   * Performs the delete cluster action.
   *
   * @param context the {@link ProvisionerContext} for this delete operation
   * @param cluster the {@link Cluster} to be deleted
   * @param conf the {@link DataprocConf} for talking to Dataproc
   * @throws Exception if failed to delete cluster
   */
  protected abstract void doDeleteCluster(ProvisionerContext context,
      Cluster cluster, DataprocConf conf) throws Exception;

  /**
   * Returns the {@link ProvisionerSystemContext} that was passed to the {@link
   * #initialize(ProvisionerSystemContext)} method. The system properties will be reloaded via the
   * {@link ProvisionerSystemContext#reloadProperties()} method upon every time when this method is
   * called.
   */
  protected ProvisionerSystemContext getSystemContext() {
    ProvisionerSystemContext context = Objects.requireNonNull(
        systemContext,
        "System context is not available. Please make sure the initialize method has been called.");
    context.reloadProperties();
    return context;
  }

  /**
   * Provides implementation of {@link RuntimeJobManager}.
   */
  @Override
  public Optional<RuntimeJobManager> getRuntimeJobManager(ProvisionerContext context) {
    Map<String, String> properties = createContextProperties(context);
    DataprocConf conf = DataprocConf.create(properties);

    // if this system property is not provided, we will assume that ssh should be used instead of
    // runtime job manager for job launch.
    if (!conf.isRuntimeJobManagerEnabled()) {
      return Optional.empty();
    }
    try {
      String clusterName = getClusterName(context);
      String projectId = conf.getProjectId();
      String region = conf.getRegion();
      String bucket =
          conf.getGcsBucket() != null ? conf.getGcsBucket() : properties.get(DataprocUtils.BUCKET);
      return Optional.of(
          new DataprocRuntimeJobManager(
              new DataprocClusterInfo(context, clusterName, conf.getDataprocCredentials(),
                  getRootUrl(conf), projectId,
                  region, bucket, getCommonDataprocLabels(context)),
              Collections.unmodifiableMap(properties), context.getCDAPVersionInfo()));
    } catch (Exception e) {
      throw new RuntimeException("Error while getting credentials for dataproc. ", e);
    }
  }

  protected String getRootUrl(DataprocConf conf) {
    return Optional.ofNullable(conf.getRootUrl())
        .orElse(ClusterControllerSettings.getDefaultEndpoint());
  }

  @Override
  public Capabilities getCapabilities() {
    return new Capabilities(
        Collections.unmodifiableSet(new HashSet<>(Arrays.asList("fileSet", "externalDataset"))));
  }

  /**
   * Returns {@code true} if the given property name from the system context properties is a default
   * property name for the dataproc config context.
   */
  protected boolean isDefaultContextProperty(String property) {
    if (DataprocConf.CLUSTER_PROPERTIES_PATTERN.matcher(property).find()) {
      return true;
    }
    return ImmutableSet.of(DataprocConf.RUNTIME_JOB_MANAGER, DataprocUtils.BUCKET,
        DataprocConf.TOKEN_ENDPOINT_KEY,
        DataprocUtils.TROUBLESHOOTING_DOCS_URL_KEY,
        DataprocConf.ENCRYPTION_KEY_NAME, DataprocConf.ROOT_URL,
        DataprocConf.COMPUTE_HTTP_REQUEST_CONNECTION_TIMEOUT,
        DataprocConf.COMPUTE_HTTP_REQUEST_READ_TIMEOUT).contains(property);
  }

  /**
   * Returns a map of default properties to be used for {@link DataprocConf}.
   */
  protected Map<String, String> getDefaultContextProperties() {
    Map<String, String> systemProps = getSystemContext().getProperties();

    // Copy set of default context properties from the system context
    return systemProps.entrySet().stream()
        .filter(e -> e.getValue() != null)
        .filter(e -> isDefaultContextProperty(e.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  protected final Map<String, String> createContextProperties(ProvisionerContext context) {
    Map<String, String> contextProperties = new HashMap<>(context.getProperties());

    // Set the project id based on the context if needed
    String projectId = getProjectId(context);
    if (projectId != null) {
      LOG.trace("Setting Dataproc project ID to {}", projectId);
      contextProperties.put(DataprocConf.PROJECT_ID_KEY, projectId);
    }

    // Add default properties
    getDefaultContextProperties().entrySet().stream()
        .filter(e -> !contextProperties.containsKey(e.getKey()))
        .forEach(e -> contextProperties.put(e.getKey(), e.getValue()));
    return contextProperties;
  }

  /**
   * Returns a set of system labels that should be applied to all Dataproc entities.
   */
  protected final Map<String, String> getCommonDataprocLabels(ProvisionerContext provisionerContext) {
    Map<String, String> labels = new HashMap<>();
    // Add labels from provisioner properties.
    // labels are expected to be in format:
    // name1|val1;name2|val2
    // Note that the delimiters for provisioner are different from the labels
    // specified in cdap-site. This is to ensure consistent delimiters for
    // provisioner properties.
    String provisionerLabelsStr = provisionerContext.getProperties().get(LABELS_PROPERTY);
    // the UI never sends nulls, it only sends empty strings. We need to ignore
    // both null and empty strings.
    if (!Strings.isNullOrEmpty(provisionerLabelsStr)) {
      labels.putAll(
          DataprocUtils.parseLabels(provisionerLabelsStr, ";", "|"));
    }

    // Add system labels.
    ProvisionerSystemContext systemContext = getSystemContext();

    // dataproc only allows label values to be lowercase letters, numbers, or dashes
    labels.put(LABEL_VERSON, getVersionLabel());
    String extraLabelsStr = systemContext.getProperties().get(LABELS_PROPERTY);

    // labels are expected to be in format:
    // name1=val1,name2=val2
    if (extraLabelsStr != null) {
      labels.putAll(DataprocUtils.parseLabels(extraLabelsStr, ",", "="));
    }
    return Collections.unmodifiableMap(labels);
  }

  protected String getVersionLabel() {
    String cdapVersion = systemContext.getCDAPVersion().toLowerCase();
    cdapVersion = cdapVersion.replaceAll("\\.", "_");
    return cdapVersion;
  }

  /**
   * Returns the project id based on the given context and the system context. If none is provided,
   * a {@code null} value will be returned.
   */
  @Nullable
  private String getProjectId(ProvisionerContext context) {
    // Default the project id from system config if missing or if it is auto-detect
    String projectId = context.getProperties().get(DataprocConf.PROJECT_ID_KEY);
    if (Strings.isNullOrEmpty(projectId) || DataprocConf.AUTO_DETECT.equals(projectId)) {
      projectId = getSystemContext().getProperties()
          .getOrDefault(DataprocConf.PROJECT_ID_KEY, projectId);
    }
    return projectId;
  }

  @Nullable
  protected DataprocImageVersion extractVersion(String imageVersion) {
    try {
      // Test simple version numbers (e.g. 1.3 1.5 2.0)
      Matcher simpleVersionMatcher = SIMPLE_VERSION_PATTERN.matcher(imageVersion);
      if (simpleVersionMatcher.matches()) {
        String version = simpleVersionMatcher.group(1);
        return new DataprocImageVersion(version);
      }

      // Test dataproc versions (e.g. 2.0.37-debian10)
      Matcher clusterVersionMatcher = CLUSTER_VERSION_PATTERN.matcher(imageVersion);
      if (clusterVersionMatcher.matches()) {
        String version = clusterVersionMatcher.group(1);
        return new DataprocImageVersion(version);
      }
    } catch (IllegalArgumentException iae) {
      LOG.warn("Unable to determine dataproc image version for image version string {}",
          imageVersion, iae);
    }

    return null;
  }
}
