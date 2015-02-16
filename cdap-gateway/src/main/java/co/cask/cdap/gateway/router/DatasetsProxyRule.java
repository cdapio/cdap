/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.router;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import org.apache.commons.lang.StringUtils;
import org.jboss.netty.handler.codec.http.HttpRequest;

import java.net.URI;

/**
 * We hide internal namespacing of datasets from user, so we want to namespace it here.
 */
public class DatasetsProxyRule implements ProxyRule {
  private final DefaultDatasetNamespace namespace;

  public DatasetsProxyRule(CConfiguration conf) {
    this.namespace = new DefaultDatasetNamespace(conf, Namespace.USER);
  }

  @Override
  public HttpRequest apply(HttpRequest request) {
    String path = URI.create(request.getUri()).normalize().getPath();
    String[] uriParts = StringUtils.split(path, '/');
    if (uriParts[0].equals(Constants.Gateway.API_VERSION_2_TOKEN)) {
      return applyToV2(request, uriParts, path);
    } else if (uriParts[0].equals(Constants.Gateway.API_VERSION_3_TOKEN)) {
      return applyToV3(request, uriParts, path);
    }
    return request;
  }

  private HttpRequest applyToV2(HttpRequest request, String [] uriParts, String path) {
    if ((uriParts.length >= 4) && uriParts[1].equals("data") && uriParts[2].equals("datasets")) {
      // three parts with '/' wrapping them
      int insertAt = uriParts[0].length() + uriParts[1].length() + uriParts[2].length() + 4;
      String datasetName = uriParts[3];
      request.setUri(processDatasetPath(path, insertAt, datasetName));
    } else if ((uriParts.length == 6) && uriParts[1].equals("data") && uriParts[2].equals("explore")
      && uriParts[3].equals("datasets") && uriParts[5].equals("schema")) {
      // four parts with '/' wrapping them
      int insertAt = uriParts[0].length() + uriParts[1].length() + uriParts[2].length() + uriParts[3].length() + 5;
      String datasetName = uriParts[4];
      request.setUri(processDatasetPath(path, insertAt, datasetName));
    }
    return request;
  }

  private HttpRequest applyToV3(HttpRequest request, String [] uriParts, String path) {
    if ((uriParts.length >= 6) && uriParts[3].equals("data") && uriParts[4].equals("datasets")) {
      // five parts with '/' wrapping them
      int insertAt = uriParts[0].length() + uriParts[1].length() + uriParts[2].length() + uriParts[3].length() +
        uriParts[4].length() + 6;
      String datasetName = uriParts[5];
      request.setUri(processDatasetPath(path, insertAt, datasetName));
    } else if ((uriParts.length == 8) && uriParts[3].equals("data") && uriParts[4].equals("explore")
      && uriParts[5].equals("datasets") && uriParts[7].equals("schema")) {
      // six parts with '/' wrapping them
      int insertAt = uriParts[0].length() + uriParts[1].length() + uriParts[2].length() + uriParts[3].length() +
        uriParts[4].length() + uriParts[5].length() + 7;
      String datasetName = uriParts[6];
      request.setUri(processDatasetPath(path, insertAt, datasetName));
    }
    return request;
  }

  private String processDatasetPath(String path, int insertAt, String datasetName) {
    String newPath = path.substring(0, insertAt) + namespace.namespace(datasetName);
    if (insertAt + datasetName.length() < path.length()) {
      int copyAfter = insertAt + datasetName.length();
      newPath = newPath + path.substring(copyAfter);
    }
    return newPath;
  }
}
