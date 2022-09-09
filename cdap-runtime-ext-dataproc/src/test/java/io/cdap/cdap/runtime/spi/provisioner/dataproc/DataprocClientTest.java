/*
 * Copyright © 2018-2022 Cask Data, Inc.
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

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.api.core.ApiFuture;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.httpjson.HttpJsonStatusCode;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.rpc.ApiException;
import com.google.api.services.compute.Compute;
import com.google.cloud.dataproc.v1.Cluster;
import com.google.cloud.dataproc.v1.ClusterControllerClient;
import com.google.cloud.dataproc.v1.ClusterOperationMetadata;
import com.google.cloud.dataproc.v1.DeleteClusterRequest;
import com.google.protobuf.Empty;
import io.cdap.cdap.runtime.spi.provisioner.RetryableProvisionException;
import io.grpc.Status;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@PrepareForTest({DataprocClient.class, ClusterControllerClient.class})
@RunWith(PowerMockRunner.class)
public class DataprocClientTest {

  @Mock
  private Compute computeMock;

  @Mock
  private ClusterControllerClient clusterControllerClientMock;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private DataprocClientFactory sshDataprocClientFactory;
  private DataprocClientFactory mockDataprocClientFactory;

  private Compute.Networks.List listMock;
  private DataprocConf dataprocConf;

  @Before
  public void init() throws Exception {

    Map<String, String> properties = new HashMap<>();
    properties.put("accountKey", "{ \"type\": \"test\"}");
    properties.put(DataprocConf.PROJECT_ID_KEY, "dummy-project");
    properties.put("zone", "us-test1-c");
    dataprocConf = DataprocConf.create(properties);

    sshDataprocClientFactory = (conf, requireSSH) ->
      new SSHDataprocClient(dataprocConf, clusterControllerClientMock, dconf -> computeMock);
    mockDataprocClientFactory = (conf, requireSSH) ->
      new MockDataprocClient(dataprocConf, clusterControllerClientMock, dconf -> computeMock);

    Compute.Networks networksMock = Mockito.mock(Compute.Networks.class);
    listMock = Mockito.mock(Compute.Networks.List.class);
    Mockito.when(computeMock.networks()).thenReturn(networksMock);
    Mockito.when(networksMock.list(Mockito.any())).thenReturn(listMock);
    Mockito.when(clusterControllerClientMock.getCluster(Mockito.any())).thenReturn(Cluster.newBuilder().build());

  }

  @Test
  public void testReadTimeOutThrowsRetryableException() throws Exception {
    Mockito.when(listMock.execute()).thenThrow(SocketTimeoutException.class);
    DataprocClient client = sshDataprocClientFactory.create(dataprocConf);
    thrown.expect(RetryableProvisionException.class);
    thrown.expectCause(IsInstanceOf.instanceOf(SocketTimeoutException.class));
    client.createCluster("name", "2.0", Collections.emptyMap(), true, null);
  }

  @Test
  public void rateLimitThrowsRetryableException() throws Exception {

    List<GoogleJsonError.ErrorInfo> errorList = new ArrayList<>();

    GoogleJsonError.ErrorInfo errorInfo = new GoogleJsonError.ErrorInfo();
    errorInfo.setReason("rateLimitExceeded");
    errorList.add(errorInfo);

    GoogleJsonError googleJsonError = new GoogleJsonError();
    googleJsonError.setErrors(errorList);

    HttpResponseException.Builder builder =
      new HttpResponseException.Builder(403, "", new HttpHeaders());

    GoogleJsonResponseException gError = new GoogleJsonResponseException(builder, googleJsonError);

    Mockito.when(listMock.execute()).thenThrow(gError);
    DataprocClient client = sshDataprocClientFactory.create(dataprocConf);
    thrown.expect(RetryableProvisionException.class);
    thrown.expectCause(IsInstanceOf.instanceOf(GoogleJsonResponseException.class));
    client.createCluster("name", "2.0", Collections.emptyMap(), true, null);
  }

  @Test
  public void nonRateLimitDoesNotThrowsRetryableException() throws Exception {

    List<GoogleJsonError.ErrorInfo> errorList = new ArrayList<>();

    GoogleJsonError.ErrorInfo errorInfo = new GoogleJsonError.ErrorInfo();
    errorInfo.setReason("NON-rateLimitExceeded");
    errorList.add(errorInfo);

    GoogleJsonError googleJsonError = new GoogleJsonError();
    googleJsonError.setErrors(errorList);

    HttpResponseException.Builder builder =
      new HttpResponseException.Builder(403, "", new HttpHeaders());

    GoogleJsonResponseException gError = new GoogleJsonResponseException(builder, googleJsonError);

    Mockito.when(listMock.execute()).thenThrow(gError);
    DataprocClient client = sshDataprocClientFactory.create(dataprocConf);
    thrown.expect(GoogleJsonResponseException.class);
    client.createCluster("name", "2.0", Collections.emptyMap(), true, null);
  }



  @Test
  public void apiExceptionWithNon4XXThrowsRetryableException() throws Exception {
    //500
    ApiException e = new ApiException(new Throwable(), GrpcStatusCode.of(Status.Code.UNKNOWN), true);

    PowerMockito.when(clusterControllerClientMock.listClusters(Mockito.anyString(), Mockito.anyString(),
                                                               Mockito.anyString()))
      .thenThrow(e);
    thrown.expect(RetryableProvisionException.class);
    thrown.expectCause(IsInstanceOf.instanceOf(ApiException.class));
    sshDataprocClientFactory.create(dataprocConf).getClusters(null, new HashMap<>());
  }

  @Test
  public void apiExceptionWith4XXNotThrowRetryableException() throws Exception {
    //500
    ApiException e = new ApiException(new Throwable(), GrpcStatusCode.of(Status.Code.UNAUTHENTICATED), true);

    PowerMockito.when(clusterControllerClientMock.listClusters(Mockito.anyString(), Mockito.anyString(),
                                                               Mockito.anyString()))
      .thenThrow(e);
    thrown.expect(DataprocRuntimeException.class);
    thrown.expectMessage("Dataproc operation failure");
    thrown.expectCause(IsInstanceOf.instanceOf(ApiException.class));
    sshDataprocClientFactory.create(dataprocConf).getClusters(null, new HashMap<>());
  }

  @Test
  public void testCreateClusterThrowsDataprocRetryableException() throws Exception {
    OperationFuture<Cluster, ClusterOperationMetadata> operationFuture = Mockito.mock(OperationFuture.class);
    Mockito.when(clusterControllerClientMock.createClusterAsync(Matchers.eq(dataprocConf.getProjectId()),
                                                                Matchers.eq(dataprocConf.getRegion()),
                                                                Mockito.any(Cluster.class)))
      .thenReturn(operationFuture);
    ApiFuture<ClusterOperationMetadata> apiFuture = Mockito.mock(ApiFuture.class);
    String errorMessage = "Connection reset by peer";
    ApiException apiException = new ApiException(new IOException(errorMessage),
                                                 HttpJsonStatusCode.of(503),
                                                 true);
    Mockito.when(apiFuture.get()).thenThrow(new ExecutionException(apiException));
    Mockito.when(operationFuture.getMetadata()).thenReturn(apiFuture);
    String operationId = "projects/proj/regions/us-east1/operations/myop";
    Mockito.when(operationFuture.getName()).thenReturn(operationId);
    thrown.expect(DataprocRetryableException.class);
    thrown.expectMessage(String.format("Dataproc operation %s failure: %s", operationId, errorMessage));
    thrown.expectCause(IsInstanceOf.instanceOf(ApiException.class));
    mockDataprocClientFactory.create(dataprocConf).createCluster("name", "2.0",
                                                                 Collections.emptyMap(), true, null);
  }

  @Test
  public void testCreateClusterThrowsDataprocRuntimeException() throws Exception {
    OperationFuture<Cluster, ClusterOperationMetadata> operationFuture = Mockito.mock(OperationFuture.class);
    Mockito.when(clusterControllerClientMock.createClusterAsync(Matchers.eq(dataprocConf.getProjectId()),
                                                                Matchers.eq(dataprocConf.getRegion()),
                                                                Mockito.any(Cluster.class)))
      .thenReturn(operationFuture);
    ApiFuture<ClusterOperationMetadata> apiFuture = Mockito.mock(ApiFuture.class);
    String errorMessage = "Operation not found";
    ApiException apiException = new ApiException(new IOException(errorMessage),
                                                 HttpJsonStatusCode.of(404),
                                                 false);
    Mockito.when(apiFuture.get()).thenThrow(new ExecutionException(apiException));
    Mockito.when(operationFuture.getMetadata()).thenReturn(apiFuture);
    String operationId = "projects/proj/regions/us-east1/operations/myop";
    Mockito.when(operationFuture.getName()).thenReturn(operationId);
    thrown.expect(DataprocRuntimeException.class);
    thrown.expectMessage(String.format("Dataproc operation %s failure: %s", operationId, errorMessage));
    thrown.expectCause(IsInstanceOf.instanceOf(ApiException.class));
    mockDataprocClientFactory.create(dataprocConf).createCluster("name", "2.0",
                                                                 Collections.emptyMap(), true, null);
  }

  @Test
  public void testDeleteClusterThrowsException() throws Exception {
    OperationFuture<Empty, ClusterOperationMetadata> operationFuture = Mockito.mock(OperationFuture.class);
    String clusterName = "mycluster";
    DeleteClusterRequest request = DeleteClusterRequest.newBuilder()
      .setClusterName(clusterName)
      .setProjectId(dataprocConf.getProjectId())
      .setRegion(dataprocConf.getRegion())
      .build();
    Mockito.when(clusterControllerClientMock.deleteClusterAsync(request))
      .thenReturn(operationFuture);
    ApiFuture<ClusterOperationMetadata> apiFuture = Mockito.mock(ApiFuture.class);
    String errorMessage = "Connection reset by peer";
    Mockito.when(apiFuture.get()).thenThrow(new ExecutionException(new IOException(errorMessage)));
    Mockito.when(operationFuture.getMetadata()).thenReturn(apiFuture);
    String operationId = "projects/proj/regions/us-east1/operations/myop";
    Mockito.when(operationFuture.getName()).thenReturn(operationId);
    thrown.expect(DataprocRuntimeException.class);
    thrown.expectMessage(String.format("Dataproc operation %s failure: %s", operationId, errorMessage));
    thrown.expectCause(IsInstanceOf.instanceOf(IOException.class));
    mockDataprocClientFactory.create(dataprocConf).deleteCluster(clusterName);
  }
}
