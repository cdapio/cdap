/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.gateway.handlers.util.VersionHelper;
import io.cdap.cdap.proto.ClientVersion;
import io.cdap.cdap.proto.Version;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Handles version requests.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class VersionHandler extends AbstractHttpHandler {

  private static final Gson GSON = new Gson();

  private final List<ClientVersion> versions;

  @Inject
  public VersionHandler(CConfiguration cConf) {
    this.versions = determineVersions(cConf);
  }

  @Path("/version")
  @GET
  public void version(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder) {
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(new Version(versions.get(0).getVersion())));
  }

  @Path("/versions")
  @GET
  public void versions(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder) {
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(versions));
  }

  private List<ClientVersion> determineVersions(CConfiguration cConf) {
    List<ClientVersion> versions = new ArrayList<>();
    versions.add(VersionHelper.getCDAPVersion());
    versions.add(VersionHelper.getSparkVersion(cConf));
    versions.add(VersionHelper.getZooKeeperVersion());
    versions.add(VersionHelper.getHadoopVersion());
    return versions;
  }
}
