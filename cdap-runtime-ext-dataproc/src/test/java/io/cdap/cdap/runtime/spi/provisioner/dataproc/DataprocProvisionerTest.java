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

package io.cdap.cdap.runtime.spi.provisioner.dataproc;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.dataproc.v1.ClusterOperationMetadata;
import com.google.cloud.dataproc.v1.ClusterStatus.State;
import com.google.common.collect.ImmutableMap;
import com.google.longrunning.Operation;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCategory.ErrorCategoryEnum;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.runtime.spi.MockVersionInfo;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.SparkCompat;
import io.cdap.cdap.runtime.spi.common.DataprocImageVersion;
import io.cdap.cdap.runtime.spi.common.DataprocMetric;
import io.cdap.cdap.runtime.spi.common.DataprocUtils;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.ClusterStatus;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Tests for Dataproc provisioner
 */
@RunWith(MockitoJUnitRunner.class)
public class DataprocProvisionerTest {
  private static final String RESOURCE_MAX_PERCENT_KEY =
    "capacity-scheduler:yarn.scheduler.capacity.maximum-am-resource-percent";
  private static final String RESOURCE_MAX_PERCENT_VAL = "0.5";
  private static final String CLUSTER_META_DATA =
      "metadata-key1|metadata-val1;metadata-key2|metadata-val2";
  private static final String SERVICE_ACCOUNT = "service-account-1";
  private static final String tokTOKEN_ENDPOINTnEndpoint = "end-point1";

  @Mock
  private DataprocClient dataprocClient;
  @Mock
  private Cluster cluster;
  @Mock
  private Cluster cluster2;

  private DataprocProvisioner provisioner;
  @Captor
  private ArgumentCaptor<Map<String, String>> addedLabelsCaptor;

  MockProvisionerContext context = new MockProvisionerContext();

  @Before
  public void init() {
    provisioner = new DataprocProvisioner((conf, requireSsh, errorCategory) -> dataprocClient);
    MockProvisionerSystemContext provisionerSystemContext = new MockProvisionerSystemContext();

    // default system properties defined by DataprocProvisioner
    provisionerSystemContext.addProperty(DataprocConf.NETWORK, "old-network");
    provisionerSystemContext.addProperty(DataprocConf.STACKDRIVER_LOGGING_ENABLED, "true");
    provisionerSystemContext
      .addProperty(DataprocConf.CLUSTER_META_DATA, CLUSTER_META_DATA);
    provisionerSystemContext.addProperty(DataprocConf.SERVICE_ACCOUNT, SERVICE_ACCOUNT);
    provisionerSystemContext.addProperty(DataprocConf.TOKEN_ENDPOINT_KEY, tokTOKEN_ENDPOINTnEndpoint);

    // default system properties defined by AbstractDataprocProvisioner
    provisionerSystemContext
        .addProperty(RESOURCE_MAX_PERCENT_KEY, RESOURCE_MAX_PERCENT_VAL);
    provisionerSystemContext
        .addProperty(DataprocConf.RUNTIME_JOB_MANAGER, "job_manager");
    provisionerSystemContext
        .addProperty("labels", "system-prop=value,user=system");
    // non-default system properties
    provisionerSystemContext.addProperty("non-system-default-key", "any-value");
    provisionerSystemContext.setCDAPVersion("6.4");

    provisioner.initialize(provisionerSystemContext);
  }

  @Test
  public void testRunKey() throws Exception {
    // test basic
    ProgramRunInfo programRunInfo = new ProgramRunInfo.Builder()
      .setNamespace("ns")
      .setApplication("app")
      .setVersion("1.0")
      .setProgramType("workflow")
      .setProgram("program")
      .setRun(UUID.randomUUID().toString())
      .build();
    Assert.assertEquals("cdap-app-" + programRunInfo.getRun(),
        new DataprocProvisioner().getRunKey(
            new MockProvisionerContext(programRunInfo)));

    // test lowercasing, stripping of invalid characters, and truncation
    programRunInfo = new ProgramRunInfo.Builder()
      .setNamespace("ns")
      .setApplication("My@Appl!cation")
      .setVersion("1.0")
      .setProgramType("workflow")
      .setProgram("program")
      .setRun(UUID.randomUUID().toString())
      .build();
    Assert.assertEquals("cdap-myapplcat-" + programRunInfo.getRun(),
        new DataprocProvisioner().getRunKey(
            new MockProvisionerContext(programRunInfo)));
  }

  @Test
  public void testDataprocConf() {
    Map<String, String> props = new HashMap<>();
    String scopes = String.format("%s,https://www.googleapis.com/auth/drive",
        DataprocConf.CLOUD_PLATFORM_SCOPE);

    props.put(DataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    props.put("zone", "region1-a");
    props.put("network", "network");
    props.put("spark:spark.reducer.maxSizeInFlight", "100");
    props.put("hadoop-env:MAPREDUCE_CLASSPATH", "xyz");
    props.put("dataproc:am.primary_only", "true");
    props.put("clusterMetaData",
        "metadata-key1|metadata-val1;metadata-key2|metadata-val2");
    props.put("clusterLabels", "label-key1|label-val1;label-key2|label-val2");
    props.put("token.endpoint", "point1");
    props.put("secureBootEnabled", "false");
    props.put("vTpmEnabled", "true");
    props.put("integrityMonitoringEnabled", "true");
    props.put("idleTTL", "20");
    props.put("clusterReuseRetryDelayMs", "20");
    props.put("clusterReuseRetryMaxMs", "200");
    props.put(DataprocConf.SCOPES, scopes);

    DataprocConf conf = DataprocConf.create(props);

    Assert.assertEquals("pid", conf.getProjectId());
    Assert.assertEquals("region1", conf.getRegion());
    Assert.assertEquals("region1-a", conf.getZone());
    Assert.assertEquals("point1", conf.getTokenEndpoint());
    Assert.assertEquals(20, conf.getIdleTtlMinutes());
    Map<String, String> clusterMetaData = conf.getClusterMetaData();
    Assert.assertEquals("metadata-val1", clusterMetaData.get("metadata-key1"));
    Assert.assertEquals("metadata-val2", clusterMetaData.get("metadata-key2"));

    Map<String, String> clusterLabels = conf.getClusterLabels();
    Assert.assertEquals("label-val1", clusterLabels.get("label-key1"));
    Assert.assertEquals("label-val2", clusterLabels.get("label-key2"));

    Map<String, String> dataprocProps = conf.getClusterProperties();
    Assert.assertEquals(3, dataprocProps.size());

    Assert.assertEquals("100",
        dataprocProps.get("spark:spark.reducer.maxSizeInFlight"));
    Assert.assertEquals("xyz",
        dataprocProps.get("hadoop-env:MAPREDUCE_CLASSPATH"));
    Assert.assertEquals("true", dataprocProps.get("dataproc:am.primary_only"));

    Assert.assertFalse(conf.isSecureBootEnabled());
    Assert.assertTrue(conf.isvTpmEnabled());
    Assert.assertTrue(conf.isIntegrityMonitoringEnabled());

    Assert.assertEquals(20, conf.getClusterReuseRetryDelayMs());
    Assert.assertEquals(200, conf.getClusterReuseRetryMaxMs());
    Assert.assertEquals(scopes, String.join(",", conf.getScopes()));
  }

  @Test
  public void testDataprocConfDefaultIdleTtl() {
    Map<String, String> props = new HashMap<>();
    props.put(DataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    DataprocConf conf = DataprocConf.create(props);
    Assert.assertEquals(30, conf.getIdleTtlMinutes());
  }

  @Test
  public void testAutoZone() {
    Map<String, String> props = new HashMap<>();
    props.put(DataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    props.put("zone", "auto-detect");
    props.put("network", "network");

    DataprocConf conf = DataprocConf.create(props);
    Assert.assertNull(conf.getZone());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidZoneCheck() {
    Map<String, String> props = new HashMap<>();
    props.put(DataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    props.put("zone", "region2-a");
    props.put("network", "network");

    DataprocConf.create(props);
  }

  @Test
  public void testCreateContextProperties() {
    final String network = "test-network";
    context.addProperty(DataprocConf.NETWORK, network);

    Map<String, String> properties = provisioner.createContextProperties(
        context);

    Assert.assertEquals(network, properties.get(DataprocConf.NETWORK));
    Assert.assertEquals("true",
        properties.get(DataprocConf.STACKDRIVER_LOGGING_ENABLED));
    Assert.assertEquals(RESOURCE_MAX_PERCENT_VAL,
        properties.get(RESOURCE_MAX_PERCENT_KEY));
    Assert.assertEquals(CLUSTER_META_DATA,
        properties.get(DataprocConf.CLUSTER_META_DATA));
    Assert.assertEquals(SERVICE_ACCOUNT,
        properties.get(DataprocConf.SERVICE_ACCOUNT));
    Assert.assertEquals(tokTOKEN_ENDPOINTnEndpoint,
        properties.get(DataprocConf.TOKEN_ENDPOINT_KEY));
    Assert.assertEquals("job_manager",
        properties.get(DataprocConf.RUNTIME_JOB_MANAGER));
    Assert.assertNull(properties.get("non-system-default-key"));
  }

  @Test
  public void testCustomImageUri() {
    Map<String, String> props = new HashMap<>();
    String customUri = "https://www.googleapis.com/compute/v1/projects/p1/global/images/testimage";
    props.put(DataprocConf.CUSTOM_IMAGE_URI, customUri);
    props.put("accountKey", "key");
    props.put("projectId", "my project");
    props.put("zone", "region1-a");

    DataprocConf conf = DataprocConf.create(props);
    Assert.assertEquals(customUri, conf.getCustomImageUri());
  }

  @Test
  public void testGetImageVersion() {
    DataprocConf defaultConf = DataprocConf.create(ImmutableMap.of(
      "accountKey", "key",
      "projectId", "my project",
      "zone", "region1-a"
    ));
    DataprocConf explicitVersionConf = DataprocConf.create(ImmutableMap.of(
      "accountKey", "key",
      "projectId", "my project",
      "zone", "region1-a",
      DataprocConf.IMAGE_VERSION, "explicit"
    ));

    context.setSparkCompat(SparkCompat.SPARK3_2_12);
    Assert.assertEquals("2.3", provisioner.getImageVersion(context, defaultConf));
    Assert.assertEquals("explicit", provisioner.getImageVersion(context, explicitVersionConf));

    context.setAppCDAPVersionInfo(new MockVersionInfo("6.5.0"));
    Assert.assertEquals("2.3", provisioner.getImageVersion(context, defaultConf));
    Assert.assertEquals("explicit", provisioner.getImageVersion(context, explicitVersionConf));

    context.setAppCDAPVersionInfo(new MockVersionInfo("6.4.0"));
    Assert.assertEquals("2.3", provisioner.getImageVersion(context, defaultConf));
    Assert.assertEquals("explicit", provisioner.getImageVersion(context, explicitVersionConf));

    //Doublecheck we still get 2.1 for Spark 3 even with CDAP 6.4
    context.setSparkCompat(SparkCompat.SPARK3_2_12);
    Assert.assertEquals("2.3", provisioner.getImageVersion(context, defaultConf));

  }

  @Test
  public void testClusterCreateNoReuse() throws Exception {
    context.addProperty("accountKey", "testKey");
    context.addProperty(DataprocConf.PROJECT_ID_KEY, "testProject");
    context.addProperty("region", "testRegion");
    context.addProperty("idleTTL", "5");
    context.addProperty(DataprocConf.SKIP_DELETE, "true");
    context.setProfileName("testProfile");
    ProgramRunInfo programRunInfo = new ProgramRunInfo.Builder()
      .setNamespace("ns")
      .setApplication("app")
      .setVersion("1.0")
      .setProgramType("workflow")
      .setProgram("program")
      .setRun("runId")
      .build();
    context.setProgramRunInfo(programRunInfo);
    context.setSparkCompat(SparkCompat.SPARK3_2_12);
    context.addProperty(DataprocConf.CLUSTER_REUSE_ENABLED, "false");
    context.setErrorCategory(new ErrorCategory(ErrorCategoryEnum.PROVISIONING));

    Mockito.when(dataprocClient.getCluster("cdap-app-runId"))
        .thenReturn(Optional.empty());
    Mockito.when(dataprocClient.createCluster(Mockito.eq("cdap-app-runId"),
            Mockito.eq("2.3"), addedLabelsCaptor.capture(), Mockito.eq(false),
            Mockito.any()))
      .thenReturn(ClusterOperationMetadata.getDefaultInstance());
    Cluster expectedCluster = new Cluster(
      "cdap-app-runId", ClusterStatus.CREATING, Collections.emptyList(), Collections.emptyMap());
    Assert.assertEquals(expectedCluster, provisioner.createCluster(context));
    HashMap<String, String> expectedLables = new HashMap<>();
    expectedLables.put("cdap-version", "6_4");
    expectedLables.put("system-prop", "value");
    expectedLables.put("user", "system");
    Assert.assertEquals(expectedLables,
        addedLabelsCaptor.getValue());
  }

  @Test
  public void testClusterReuseOnCreate() throws Exception {
    context.addProperty("accountKey", "testKey");
    context.addProperty(DataprocConf.PROJECT_ID_KEY, "testProject");
    context.addProperty("region", "testRegion");
    context.addProperty("idleTTL", "5");
    context.addProperty(DataprocConf.SKIP_DELETE, "true");
    context.setProfileName("testProfile");
    context.addProperty(DataprocConf.CLUSTER_REUSE_RETRY_DELAY_MS, "0");
    ProgramRunInfo programRunInfo = new ProgramRunInfo.Builder()
      .setNamespace("ns")
      .setApplication("app")
      .setVersion("1.0")
      .setProgramType("workflow")
      .setProgram("program")
      .setRun("runId")
      .build();
    context.setProgramRunInfo(programRunInfo);
    context.setErrorCategory(new ErrorCategory(ErrorCategoryEnum.PROVISIONING));

    //A. Check with existing client, probably after a retry
    Mockito.when(dataprocClient.getClusters(
            Collections.singletonMap(AbstractDataprocProvisioner.LABEL_RUN_KEY, "cdap-app-runId")))
        .thenAnswer(i -> Stream.of(cluster));
    Mockito.when(cluster.getStatus())
        .thenReturn(ClusterStatus.RUNNING);
    Assert.assertEquals(cluster, provisioner.createCluster(context));

    //B. With preallocated cluster in "bad" state new allocation should happen.
    Mockito.when(cluster.getStatus())
        .thenReturn(ClusterStatus.FAILED);
    Mockito.when(cluster2.getName())
        .thenReturn("cluster2");
    DataprocConf conf = DataprocConf.create(
        provisioner.createContextProperties(context));
    ImmutableMap<String, String> reuseClusterFilter = ImmutableMap.of(
        AbstractDataprocProvisioner.LABEL_VERSON, "6_4",
        AbstractDataprocProvisioner.LABEL_REUSE_KEY, conf.getClusterReuseKey(),
        AbstractDataprocProvisioner.LABEL_PROFILE, "testProfile");

    Mockito.when(dataprocClient.getClusters(Mockito.eq(reuseClusterFilter), Mockito.any()))
        //B.1. When there is no good cluster found, a retry should happen
        .thenAnswer(i -> {
          //Ensure we call the predicate
          Predicate clusterPredicate = i.getArgument(1, Predicate.class);
          com.google.cloud.dataproc.v1.Cluster updatingCluster = com.google.cloud.dataproc.v1.Cluster.newBuilder()
              .setStatus(com.google.cloud.dataproc.v1.ClusterStatus.newBuilder()
                  .setState(State.UPDATING))
              .build();
          com.google.cloud.dataproc.v1.Cluster deletingCluster = com.google.cloud.dataproc.v1.Cluster.newBuilder()
              .setStatus(com.google.cloud.dataproc.v1.ClusterStatus.newBuilder()
                  .setState(State.DELETING))
              .build();
          Assert.assertFalse(clusterPredicate.test(updatingCluster));
          Assert.assertFalse(clusterPredicate.test(deletingCluster));
          return Stream.empty();
        })
        //B.2. Finally a reuse cluster found
        .thenAnswer(i -> Stream.of(cluster2));

    Assert.assertEquals(cluster2, provisioner.createCluster(context));

    Mockito.verify(dataprocClient)
        .updateClusterLabels("cluster2",
            Collections.singletonMap(AbstractDataprocProvisioner.LABEL_RUN_KEY, "cdap-app-runId"),
            Collections.singleton(AbstractDataprocProvisioner.LABEL_REUSE_UNTIL));
  }

  @Test
  public void testClusterMarkedForReuseOnDelete() throws Exception {
    context.addProperty("accountKey", "testKey");
    context.addProperty(DataprocConf.PROJECT_ID_KEY, "testProject");
    context.addProperty("region", "testRegion");
    context.addProperty("idleTTL", "5");
    context.addProperty(DataprocConf.SKIP_DELETE, "true");
    context.setErrorCategory(new ErrorCategory(ErrorCategoryEnum.DEPROVISIONING));
    DataprocConf conf = DataprocConf.create(
        provisioner.createContextProperties(context));
    Mockito.when(cluster.getName())
        .thenReturn("testClusterName");
    provisioner.doDeleteCluster(context, cluster, conf);

    Mockito.verify(dataprocClient)
        .updateClusterLabels(Mockito.eq("testClusterName"), addedLabelsCaptor.capture(),
            Mockito.eq(Collections.singleton(AbstractDataprocProvisioner.LABEL_RUN_KEY)));
    Assert.assertEquals(
        Collections.singleton(AbstractDataprocProvisioner.LABEL_REUSE_UNTIL),
        addedLabelsCaptor.getValue()
            .keySet());
  }

  @Test
  public void testVersionComparison() {
    DataprocImageVersion v0 = new DataprocImageVersion("0");
    DataprocImageVersion v1 = new DataprocImageVersion("1");
    DataprocImageVersion v1debian = new DataprocImageVersion("1-debian");
    DataprocImageVersion v1dot4 = new DataprocImageVersion("1.4");
    DataprocImageVersion v1dot4dot99 = new DataprocImageVersion("1.4.99");
    DataprocImageVersion v1dot5 = new DataprocImageVersion("1.5");
    DataprocImageVersion v1dot5debian = new DataprocImageVersion("1.5-debian");
    DataprocImageVersion v1dot5dot0 = new DataprocImageVersion("1.5.0");
    DataprocImageVersion v1dot5dot0debian = new DataprocImageVersion(
        "1.5.0-debian");
    DataprocImageVersion v1dot5dot0dot0 = new DataprocImageVersion("1.5.0.0");
    DataprocImageVersion v1dot5dot1 = new DataprocImageVersion("1.5.1");
    DataprocImageVersion v1dot6 = new DataprocImageVersion("1.6");
    DataprocImageVersion v2 = new DataprocImageVersion("2");
    DataprocImageVersion v2debian = new DataprocImageVersion("2-debian");
    DataprocImageVersion v2dot0 = new DataprocImageVersion("2.0");

    Assert.assertTrue(v1dot5.compareTo(v0) > 0);
    Assert.assertTrue(v1dot5.compareTo(v1) > 0);
    Assert.assertTrue(v1dot5.compareTo(v1debian) > 0);
    Assert.assertTrue(v1dot5.compareTo(v1dot4) > 0);
    Assert.assertTrue(v1dot5.compareTo(v1dot4dot99) > 0);
    Assert.assertTrue(v1dot5.compareTo(v1dot5) == 0);
    Assert.assertTrue(v1dot5.compareTo(v1dot5debian) == 0);
    Assert.assertTrue(v1dot5.compareTo(v1dot5dot0) == 0);
    Assert.assertTrue(v1dot5.compareTo(v1dot5dot0debian) == 0);
    Assert.assertTrue(v1dot5.compareTo(v1dot5dot0dot0) == 0);
    Assert.assertTrue(v1dot5.compareTo(v1dot5dot1) < 0);
    Assert.assertTrue(v1dot5.compareTo(v1dot6) < 0);
    Assert.assertTrue(v1dot5.compareTo(v2) < 0);
    Assert.assertTrue(v1dot5.compareTo(v2debian) < 0);
    Assert.assertTrue(v1dot5.compareTo(v2dot0) < 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullDataprocImageVersion() {
    DataprocImageVersion nullVersion = new DataprocImageVersion(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyDataprocImageVersion() {
    DataprocImageVersion emptyVersion = new DataprocImageVersion("");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSegmentDataprocImageVersion() {
    DataprocImageVersion emptyVersion = new DataprocImageVersion("1.2.3..6");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidDataprocImageVersion() {
    DataprocImageVersion emptyVersion = new DataprocImageVersion("abcd");
  }

  @Test
  public void testCommonDataprocLabels() {
    context.addProperty("labels", "user|cdap;program|data-pipeline");
    Map<String, String> labels = provisioner.getCommonDataprocLabels(context);
    // Check that the value from system context has overwritten the provisioner context.
    Assert.assertEquals("system", labels.get("user"));
    // Check that the CDAP version is set by system context.
    Assert.assertEquals("6_4", labels.get("cdap-version"));
    // Check that the non-overwritten value from user context is present.
    Assert.assertEquals("data-pipeline", labels.get("program"));
    // Check that the unique value from system context is present.
    Assert.assertEquals("value", labels.get("system-prop"));
  }

  @Test
  public void testCommonDataprocLabelsInvalidLabel() {
    context.addProperty("labels", "email|user@cdapio");
    Map<String, String> labels = provisioner.getCommonDataprocLabels(context);
    Assert.assertNull(labels.get("email"));
  }

  @Test
  public void testGetClusterStatusEmitLroMetricOnCreate() throws Exception {
    MockProvisionerContext localContext = new MockProvisionerContext();
    localContext.addProperty("accountKey", "testKey");
    localContext.addProperty(DataprocConf.PROJECT_ID_KEY, "testProject");
    localContext.addProperty("region", "testRegion");
    localContext.setErrorCategory(new ErrorCategory(ErrorCategoryEnum.PROVISIONING));
    localContext.setProgramRunInfo(new ProgramRunInfo.Builder()
                                     .setNamespace("ns").setApplication("app").setVersion("1.0")
                                     .setProgramType("workflow").setProgram("program").setRun("runId").build());

    Cluster cdapClusterInput = new Cluster("mycluster", ClusterStatus.CREATING,
                                           Collections.emptyList(), Collections.emptyMap());

    Mockito.when(dataprocClient.getClusterStatus(Mockito.anyString()))
      .thenReturn(ClusterStatus.RUNNING);

    Operation operation = Operation.newBuilder()
      .setName("op-create")
      .setDone(true)
      .build();

    Mockito.when(dataprocClient.getLatestOperation(Mockito.anyString(), Mockito.anyString()))
      .thenReturn(Optional.of(operation));

    ClusterStatus currentStatus = provisioner.getClusterStatus(localContext, cdapClusterInput);
    Assert.assertEquals(ClusterStatus.RUNNING, currentStatus);

    Integer count = localContext.getMetricCounts().get("provisioner.cluster.operation.response.count");
    Assert.assertNotNull("The LRO metric was not emitted or recorded in the mock context", count);
    Assert.assertEquals(1, count.intValue());
  }

  @Test
  public void testGetClusterStatusEmitLroMetricOnDelete() throws Exception {
    MockProvisionerContext localContext = new MockProvisionerContext();
    localContext.addProperty("accountKey", "testKey");
    localContext.addProperty(DataprocConf.PROJECT_ID_KEY, "testProject");
    localContext.addProperty("region", "testRegion");
    localContext.setErrorCategory(new ErrorCategory(ErrorCategoryEnum.DEPROVISIONING));
    localContext.setProgramRunInfo(new ProgramRunInfo.Builder()
                                     .setNamespace("ns").setApplication("app").setVersion("1.0")
                                     .setProgramType("workflow").setProgram("program").setRun("runId").build());

    Cluster cdapClusterInput = new Cluster("mycluster", ClusterStatus.DELETING,
                                           Collections.emptyList(), Collections.emptyMap());

    Mockito.when(dataprocClient.getClusterStatus(Mockito.anyString()))
      .thenReturn(ClusterStatus.NOT_EXISTS);

    ClusterStatus currentStatus = provisioner.getClusterStatus(localContext, cdapClusterInput);
    Assert.assertEquals(ClusterStatus.NOT_EXISTS, currentStatus);

    Integer count = localContext.getMetricCounts().get("provisioner.cluster.operation.response.count");
    Assert.assertNotNull("The LRO metric was not emitted or recorded in the mock context", count);
    Assert.assertEquals(1, count.intValue());
  }

  @Test
  public void testGetClusterStatusEmitLroMetricOnError() throws Exception {
    MockProvisionerContext localContext = new MockProvisionerContext();
    localContext.addProperty("accountKey", "testKey");
    localContext.addProperty(DataprocConf.PROJECT_ID_KEY, "testProject");
    localContext.addProperty("region", "testRegion");
    localContext.setErrorCategory(new ErrorCategory(ErrorCategoryEnum.PROVISIONING));
    localContext.setProgramRunInfo(new ProgramRunInfo.Builder()
                                     .setNamespace("ns").setApplication("app").setVersion("1.0")
                                     .setProgramType("workflow").setProgram("program").setRun("runId").build());

    Cluster cdapClusterInput = new Cluster("mycluster", ClusterStatus.CREATING,
                                           Collections.emptyList(), Collections.emptyMap());

    Mockito.when(dataprocClient.getClusterStatus(Mockito.anyString()))
      .thenReturn(ClusterStatus.FAILED);

    com.google.rpc.Status statusError = com.google.rpc.Status.newBuilder()
      .setCode(3)
      .setMessage("Invalid resource configuration")
      .build();
    Operation operation = Operation.newBuilder()
      .setName("op-create")
      .setDone(true)
      .setError(statusError)
      .build();
    Mockito.when(dataprocClient.getLatestOperation(Mockito.anyString(), Mockito.anyString()))
      .thenReturn(Optional.of(operation));

    ClusterStatus currentStatus = provisioner.getClusterStatus(localContext, cdapClusterInput);
    Assert.assertEquals(ClusterStatus.FAILED, currentStatus);

    Integer count = localContext.getMetricCounts().get("provisioner.cluster.operation.response.count");
    Assert.assertNotNull("The LRO metric was not emitted or recorded in the mock context", count);
    Assert.assertEquals(1, count.intValue());
  }

  @Test
  public void testGetClusterStatusEmitLroMetricOperationNotDone() throws Exception {
    MockProvisionerContext localContext = new MockProvisionerContext();
    localContext.addProperty("accountKey", "testKey");
    localContext.addProperty(DataprocConf.PROJECT_ID_KEY, "testProject");
    localContext.addProperty("region", "testRegion");
    localContext.setErrorCategory(new ErrorCategory(ErrorCategoryEnum.PROVISIONING));
    localContext.setProgramRunInfo(new ProgramRunInfo.Builder()
                                     .setNamespace("ns").setApplication("app").setVersion("1.0")
                                     .setProgramType("workflow").setProgram("program").setRun("runId").build());

    Cluster cdapClusterInput = new Cluster("mycluster", ClusterStatus.CREATING,
                                           Collections.emptyList(), Collections.emptyMap());

    Mockito.when(dataprocClient.getClusterStatus("mycluster"))
      .thenReturn(ClusterStatus.RUNNING);

    Operation operation = Operation.newBuilder()
      .setName("op-create")
      .setDone(false)
      .build();
    Mockito.when(dataprocClient.getLatestOperation("mycluster", "CREATE"))
      .thenReturn(Optional.of(operation));

    ClusterStatus currentStatus = provisioner.getClusterStatus(localContext, cdapClusterInput);
    Assert.assertEquals(ClusterStatus.RUNNING, currentStatus);

    Integer count = localContext.getMetricCounts().get("provisioner.cluster.operation.response.count");
    Assert.assertNull(count);
  }

  @Test
  public void testGetClusterStatusEmitLroMetricClientThrowsException() throws Exception {
    MockProvisionerContext localContext = new MockProvisionerContext();
    localContext.addProperty("accountKey", "testKey");
    localContext.addProperty(DataprocConf.PROJECT_ID_KEY, "testProject");
    localContext.addProperty("region", "testRegion");
    localContext.setErrorCategory(new ErrorCategory(ErrorCategoryEnum.PROVISIONING));
    localContext.setProgramRunInfo(new ProgramRunInfo.Builder()
                                     .setNamespace("ns").setApplication("app").setVersion("1.0")
                                     .setProgramType("workflow").setProgram("program").setRun("runId").build());

    Cluster cdapClusterInput = new Cluster("mycluster", ClusterStatus.CREATING,
                                           Collections.emptyList(), Collections.emptyMap());

    Mockito.when(dataprocClient.getClusterStatus("mycluster"))
      .thenReturn(ClusterStatus.RUNNING);

    Mockito.when(dataprocClient.getLatestOperation("mycluster", "CREATE"))
      .thenThrow(new RuntimeException("API connection failure"));

    ClusterStatus status = provisioner.getClusterStatus(localContext, cdapClusterInput);
    Assert.assertEquals(ClusterStatus.RUNNING, status);

    Integer count = localContext.getMetricCounts().get("provisioner.cluster.operation.response.count");
    Assert.assertNull(count);
  }

  @Test
  public void testDataprocUtilsEmitMetricWithExceptionBranches(){
    MockProvisionerContext localContext = new MockProvisionerContext();

    StatusCode statusCode = Mockito.mock(StatusCode.class);
    StatusCode.Code realCode = StatusCode.Code.ALREADY_EXISTS;
    Mockito.when(statusCode.getCode()).thenReturn(realCode);

    ApiException apiException = Mockito.mock(ApiException.class);
    Mockito.when(apiException.getStatusCode()).thenReturn(statusCode);

    DataprocMetric metricWithApiExp = DataprocMetric.builder("test.api.metric")
      .setRegion("us-test1")
      .setException(new Exception(apiException))
      .build();

    DataprocUtils.emitMetric(localContext, metricWithApiExp);

    DataprocMetric metricWithGenericExp = DataprocMetric.builder("test.generic.metric")
      .setRegion("us-test1")
      .setException(new RuntimeException("Generic connection failure"))
      .build();

    DataprocUtils.emitMetric(localContext, metricWithGenericExp);

    Integer apiCount = localContext.getMetricCounts().get("test.api.metric");
    Integer genericCount = localContext.getMetricCounts().get("test.generic.metric");

    Assert.assertNotNull(apiCount);
    Assert.assertNotNull(genericCount);
    Assert.assertEquals(1, apiCount.intValue());
    Assert.assertEquals(1, genericCount.intValue());
  }
  
  @Test
  public void testFlexVmConfigurationParsing() {
    Map<String, String> props = new HashMap<>();
    props.put(DataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    props.put("workerCPUs", "2");
    props.put("workerMemoryMB", "8192");
    props.put("masterCPUs", "2");
    props.put("masterMemoryMB", "8192");
    props.put(DataprocConf.WORKER_FLEX_VM_MACHINE_TYPES, "n2, e2");
    props.put(DataprocConf.MASTER_FLEX_VM_MACHINE_TYPES, "n1, n2d");

    DataprocConf conf = DataprocConf.create(props);

    List<String> expectedWorkerTypes = Arrays.asList("n2-custom-2-8192", "e2-custom-2-8192");
    Assert.assertEquals(expectedWorkerTypes, conf.getWorkerFlexVmMachineTypes());

    List<String> expectedMasterTypes = Arrays.asList("custom-2-8192", "n2d-custom-2-8192");
    Assert.assertEquals(expectedMasterTypes, conf.getMasterFlexVmMachineTypes());
  }

  @Test
  public void testFlexVmDefaultEmpty() {
    Map<String, String> props = new HashMap<>();
    props.put(DataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");

    DataprocConf conf = DataprocConf.create(props);

    Assert.assertTrue(conf.getWorkerFlexVmMachineTypes().isEmpty());
    Assert.assertTrue(conf.getMasterFlexVmMachineTypes().isEmpty());
  }

  @Test
  public void testFlexVmValidationSuccess() {
    Map<String, String> props = new HashMap<>();
    props.put(DataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    props.put(DataprocConf.WORKER_FLEX_VM_MACHINE_TYPES, "n2, e2");
    provisioner.validateProperties(props);
  }

  @Test(expected = DataprocRuntimeException.class)
  public void testFlexVmValidationFailureOnInvalidFormat() {
    Map<String, String> props = new HashMap<>();
    props.put(DataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    props.put(DataprocConf.WORKER_FLEX_VM_MACHINE_TYPES, "invalid@machine#name");

    provisioner.validateProperties(props);
  }

  @Test
  public void testFlexVmConfigurationParsingWithSpacesAndEmptyEntries() {
    Map<String, String> props = new HashMap<>();
    props.put(DataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    props.put("workerCPUs", "4");
    props.put("workerMemoryMB", "16384");
    props.put("masterCPUs", "8");
    props.put("masterMemoryMB", "32768");
    props.put(DataprocConf.WORKER_FLEX_VM_MACHINE_TYPES, " , n2 , , e2 , , c2 , ");
    props.put(DataprocConf.MASTER_FLEX_VM_MACHINE_TYPES, "  n1 , , n2d  ");

    DataprocConf conf = DataprocConf.create(props);

    List<String> expectedWorkerTypes = Arrays.asList(
      "n2-custom-4-16384", "e2-custom-4-16384", "c2-custom-4-16384");
    Assert.assertEquals(expectedWorkerTypes, conf.getWorkerFlexVmMachineTypes());

    List<String> expectedMasterTypes = Arrays.asList(
      "custom-8-32768", "n2d-custom-8-32768");
    Assert.assertEquals(expectedMasterTypes, conf.getMasterFlexVmMachineTypes());
  }

  @Test
  public void testFlexVmValidationMasterFailureOnInvalidFormat() {
    Map<String, String> props = new HashMap<>();
    props.put(DataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    props.put(DataprocConf.MASTER_FLEX_VM_MACHINE_TYPES, "invalid#master@type");

    try {
      provisioner.validateProperties(props);
      Assert.fail("Expected validation to fail for invalid master flexible VM type");
    } catch (DataprocRuntimeException e) {
      Assert.assertEquals(ErrorType.USER, e.getErrorType());
      Assert.assertEquals(DataprocRuntimeException.ERROR_CATEGORY_PROVISIONING_CONFIGURATION,
                          e.getErrorCategory());
      Assert.assertTrue(e.getMessage().contains("Invalid flexible VM machine type"));
    }
  }

  @Test
  public void testFlexVmValidationWorkerFailureDetails() {
    Map<String, String> props = new HashMap<>();
    props.put(DataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    props.put(DataprocConf.WORKER_FLEX_VM_MACHINE_TYPES, "invalid_type");

    try {
      provisioner.validateProperties(props);
      Assert.fail("Expected validation to fail for invalid worker flexible VM type");
    } catch (DataprocRuntimeException e) {
      Assert.assertEquals(ErrorType.USER, e.getErrorType());
      Assert.assertEquals(DataprocRuntimeException.ERROR_CATEGORY_PROVISIONING_CONFIGURATION,
                          e.getErrorCategory());
      Assert.assertTrue(e.getMessage().contains("Invalid flexible VM machine type"));
    }
  }

  @Test
  public void testFlexVmValidationBothMasterAndWorkerSuccess() {
    Map<String, String> props = new HashMap<>();
    props.put(DataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    props.put(DataprocConf.MASTER_FLEX_VM_MACHINE_TYPES, "n1, n2, n2d, e2");
    props.put(DataprocConf.WORKER_FLEX_VM_MACHINE_TYPES, "n1, n2, n2d, e2");

    provisioner.validateProperties(props);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testMasterFlexVmUnmodifiableList() {
    Map<String, String> props = new HashMap<>();
    props.put(DataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    props.put(DataprocConf.MASTER_FLEX_VM_MACHINE_TYPES, "n1, n2");

    DataprocConf conf = DataprocConf.create(props);
    conf.getMasterFlexVmMachineTypes().add("e2-custom-2-8192");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testWorkerFlexVmUnmodifiableList() {
    Map<String, String> props = new HashMap<>();
    props.put(DataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    props.put(DataprocConf.WORKER_FLEX_VM_MACHINE_TYPES, "n1, n2");

    DataprocConf conf = DataprocConf.create(props);
    conf.getWorkerFlexVmMachineTypes().add("e2-custom-2-8192");
  }

  @Test
  public void testNetworkTagsParsing() {
    Map<String, String> props = new HashMap<>();
    props.put(DataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    props.put("networkTags", "tag1,  tag2 , , tag3 ");

    DataprocConf conf = DataprocConf.create(props);
    Assert.assertEquals(Arrays.asList("tag1", "tag2", "tag3"), conf.getNetworkTags());
  }

  @Test
  public void testNetworkTagsDefaultEmpty() {
    Map<String, String> props = new HashMap<>();
    props.put(DataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");

    DataprocConf conf = DataprocConf.create(props);
    Assert.assertTrue(conf.getNetworkTags().isEmpty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNetworkTagsExceedsMaxLimit() {
    Map<String, String> props = new HashMap<>();
    props.put(DataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    StringBuilder tags = new StringBuilder();
    for (int i = 0; i < 65; i++) {
      tags.append("tag").append(i).append(",");
    }
    props.put("networkTags", tags.toString());

    DataprocConf.create(props);
  }

  @Test
  public void testScopesDefaultContainsCloudPlatformScope() {
    Map<String, String> props = new HashMap<>();
    props.put(DataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");

    DataprocConf conf = DataprocConf.create(props);
    Assert.assertEquals(Collections.singletonList(DataprocConf.CLOUD_PLATFORM_SCOPE), conf.getScopes());
  }

  @Test
  public void testScopesParsingOrderAndDeduplication() {
    Map<String, String> props = new HashMap<>();
    props.put(DataprocConf.PROJECT_ID_KEY, "pid");
    props.put("accountKey", "key");
    props.put("region", "region1");
    props.put(DataprocConf.SCOPES, "https://www.googleapis.com/auth/bigquery, https://www.googleapis.com/auth/drive, "
      + "https://www.googleapis.com/auth/bigquery, " + DataprocConf.CLOUD_PLATFORM_SCOPE);

    DataprocConf conf = DataprocConf.create(props);
    List<String> expectedScopes = Arrays.asList(
      "https://www.googleapis.com/auth/bigquery",
      "https://www.googleapis.com/auth/drive",
      DataprocConf.CLOUD_PLATFORM_SCOPE
    );
    Assert.assertEquals(expectedScopes, conf.getScopes());
  }
}