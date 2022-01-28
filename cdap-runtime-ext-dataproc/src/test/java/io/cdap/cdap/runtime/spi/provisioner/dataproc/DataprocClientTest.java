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
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.api.services.compute.Compute;
import com.google.cloud.dataproc.v1.ClusterControllerClient;
import io.cdap.cdap.runtime.spi.provisioner.RetryableProvisionException;
import io.grpc.Status;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@PrepareForTest({DataprocClient.class, ClusterControllerClient.class})
@RunWith(PowerMockRunner.class)
public class DataprocClientTest {

  @Mock
  private Compute computeMock;

  @Mock
  private ClusterControllerClient clusterControllerClientMock;

  private Compute.Networks.List listMock;
  private DataprocConf dataprocConf;

  @Before
  public void init() throws Exception {

    Map<String, String> properties = new HashMap<>();
    properties.put("accountKey", "{ \"type\": \"test\"}");
    properties.put(DataprocConf.PROJECT_ID_KEY, "dummy-project");
    properties.put("zone", "us-test1-c");
    dataprocConf = DataprocConf.create(properties, null);

    PowerMockito.spy(DataprocClient.class);
    PowerMockito.doReturn(clusterControllerClientMock)
      .when(DataprocClient.class, "getClusterControllerClient", Mockito.any());
    PowerMockito.doReturn(computeMock)
      .when(DataprocClient.class, "getCompute", Mockito.any());

    Compute.Networks networksMock = Mockito.mock(Compute.Networks.class);
    listMock = Mockito.mock(Compute.Networks.List.class);
    Mockito.when(computeMock.networks()).thenReturn(networksMock);
    Mockito.when(networksMock.list(Mockito.any())).thenReturn(listMock);

  }

  @Test(expected = RetryableProvisionException.class)
  public void testReadTimeOutThrowsRetryableException() throws Exception {
    Mockito.when(listMock.execute()).thenThrow(SocketTimeoutException.class);
    DataprocClient.fromConf(dataprocConf);
  }

  @Test(expected = RetryableProvisionException.class)
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
    DataprocClient.fromConf(dataprocConf);
  }

  @Test(expected = GoogleJsonResponseException.class)
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
    DataprocClient.fromConf(dataprocConf);
  }



  @Test(expected = RetryableProvisionException.class)
  public void apiExceptionWithNon4XXThrowsRetryableException() throws Exception {
    //500
    ApiException e = new ApiException(new Throwable(), GrpcStatusCode.of(Status.Code.UNKNOWN), true);

    PowerMockito.when(clusterControllerClientMock.listClusters(Mockito.anyString(), Mockito.anyString(),
                                                               Mockito.anyString()))
      .thenThrow(e);

    DataprocClient.fromConf(dataprocConf, false)
      .getClusters(null, new HashMap<>());
  }


  @Test(expected = DataprocRuntimeException.class)
  public void apiExceptionWith4XXNotThrowRetryableException() throws Exception {
    //500
    ApiException e = new ApiException(new Throwable(), GrpcStatusCode.of(Status.Code.UNAUTHENTICATED), true);

    PowerMockito.when(clusterControllerClientMock.listClusters(Mockito.anyString(), Mockito.anyString(),
                                                               Mockito.anyString()))
      .thenThrow(e);

    DataprocClient.fromConf(dataprocConf, false)
      .getClusters(null, new HashMap<>());
  }
}
