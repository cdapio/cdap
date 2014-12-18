/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.test;

import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.exception.ProgramNotFoundException;
import co.cask.cdap.client.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.exception.ResetFailureException;
import co.cask.cdap.test.exception.ResetNotEnabledException;
import co.cask.cdap.test.exception.UnauthorizedException;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class IntegrationTestClient {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestClient.class);

  private final ClientConfig config;
  private final RESTClient restClient;
  private final ApplicationClient applicationClient;
  private final ProgramClient programClient;

  public IntegrationTestClient(ClientConfig config) {
    this.config = config;
    this.restClient = new RESTClient(config.getDefaultHttpConfig(), config.getUploadHttpConfig(),
                                     config.getUnavailableRetryLimit());
    this.programClient = new ProgramClient(config);
    this.applicationClient = new ApplicationClient(config);
  }

  public void resetUnrecoverably() throws ResetFailureException, UnauthorizedException, IOException,
    UnAuthorizedAccessTokenException, ResetNotEnabledException {

    URL url = config.resolveURL(String.format("unrecoverable/reset"));
    HttpRequest request = HttpRequest.post(url).build();

    HttpResponse response = restClient.execute(request, config.getAccessToken(),
                                               HttpURLConnection.HTTP_UNAUTHORIZED, HttpURLConnection.HTTP_BAD_REQUEST,
                                               HttpURLConnection.HTTP_FORBIDDEN);
    if (response.getResponseCode() == HttpURLConnection.HTTP_UNAUTHORIZED) {
      throw new UnauthorizedException();
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new ResetFailureException(response.getResponseMessage());
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_FORBIDDEN) {
      throw new ResetNotEnabledException();
    }
  }

  public void stopAll() throws IOException, UnAuthorizedAccessTokenException, InterruptedException, TimeoutException {
    Map<ProgramType, List<ProgramRecord>> allPrograms = applicationClient.listAllPrograms();
    for (Map.Entry<ProgramType, List<ProgramRecord>> entry : allPrograms.entrySet()) {
      ProgramType programType = entry.getKey();
      List<ProgramRecord> programRecords = entry.getValue();
      for (ProgramRecord programRecord : programRecords) {
        try {
          String status = programClient.getStatus(programRecord.getApp(), programType, programRecord.getId());
          if (!status.equals("STOPPED")) {
            LOG.info("Stopping program: appId=" + programRecord.getApp()
                       + " programType=" + programType + " programId=" + programRecord.getId());
            programClient.stop(programRecord.getApp(), programType, programRecord.getId());
            programClient.waitForStatus(programRecord.getApp(), programType, programRecord.getId(),
                                        "STOPPED", 60, TimeUnit.SECONDS);
          }
        } catch (ProgramNotFoundException e) {
          LOG.warn("Error fetching the status of program: appId=" + programRecord.getApp()
                   + " programType=" + programType + " programId=" + programRecord.getId(), e);
        }
      }
    }
  }

}
