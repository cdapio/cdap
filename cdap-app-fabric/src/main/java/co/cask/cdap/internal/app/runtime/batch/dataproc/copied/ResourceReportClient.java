/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch.dataproc.copied;

import org.apache.twill.api.ResourceReport;
import org.apache.twill.internal.json.ResourceReportAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.DeflaterInputStream;
import java.util.zip.GZIPInputStream;

/**
 * Package private class to get {@link ResourceReport} from the application master.
 */
final class ResourceReportClient {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceReportClient.class);

  private final ResourceReportAdapter reportAdapter;
  private final List<URL> resourceUrls;

  ResourceReportClient(List<URL> resourceUrls) {
    this.resourceUrls = resourceUrls;
    this.reportAdapter = ResourceReportAdapter.create();
  }

  /**
   * Returns the resource usage of the application fetched from the resource endpoint URL.
   * @return A {@link ResourceReport} or {@code null} if failed to fetch the report.
   */
  public ResourceReport get() {
    for (URL url : resourceUrls) {
      try {
        HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
        urlConn.setRequestProperty("Accept-Encoding", "gzip, deflate");

        if (urlConn.getResponseCode() != 200) {
          continue;
        }

        try (Reader reader = new InputStreamReader(getInputStream(urlConn), StandardCharsets.UTF_8)) {
          LOG.trace("Report returned by {}", url);
          return reportAdapter.fromJson(reader);
        }
      } catch (IOException e) {
        // Just log a trace as it's ok to not able to fetch resource report
        LOG.trace("Exception raised when getting resource report from {}.", url, e);
      }
    }
    return null;
  }

  private InputStream getInputStream(HttpURLConnection urlConn) throws IOException {
    InputStream is = urlConn.getInputStream();
    String contentEncoding = urlConn.getContentEncoding();
    if (contentEncoding == null) {
      return is;
    }
    if ("gzip".equalsIgnoreCase(contentEncoding)) {
      return new GZIPInputStream(is);
    }
    if ("deflate".equalsIgnoreCase(contentEncoding)) {
      return new DeflaterInputStream(is);
    }
    // This should never happen
    throw new IOException("Unsupported content encoding " + contentEncoding);
  }
}
