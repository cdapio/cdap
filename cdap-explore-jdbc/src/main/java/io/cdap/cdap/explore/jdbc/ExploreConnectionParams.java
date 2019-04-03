/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.explore.jdbc;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Collection;

/**
 * Explore connection parameters.
 */
public class ExploreConnectionParams {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreConnectionParams.class);
  static final int DEFAULT_FETCH_SIZE = 1000;

  /**
   * Extra Explore connection parameter.
   */
  public enum Info {
    EXPLORE_AUTH_TOKEN("auth.token"),
    NAMESPACE("namespace"),
    SSL_ENABLED("ssl.enabled"),
    VERIFY_SSL_CERT("verify.ssl.cert"),
    FETCH_SIZE("fetch.size");

    private final String name;

    Info(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public static Info fromStr(String name) {
      for (Info info : Info.values()) {
        if (info.getName().equals(name)) {
          return info;
        }
      }
      return null;
    }
  }

  private final String host;
  private final int port;
  private final Multimap<Info, String> extraInfos;

  ExploreConnectionParams(String host, int port, Multimap<Info, String> extraInfos) {
    this.host = host;
    this.port = port;
    this.extraInfos = extraInfos;
  }

  public Multimap<Info, String> getExtraInfos() {
    return extraInfos;
  }

  public int getPort() {
    return port;
  }

  public String getHost() {
    return host;
  }

  public int getFetchSize() {
    Collection<String> fetchSizes = extraInfos.get(Info.FETCH_SIZE);
    if (fetchSizes.isEmpty()) {
      return DEFAULT_FETCH_SIZE;
    } else {
      String fetch = fetchSizes.iterator().next();
      try {
        return Integer.parseInt(fetch);
      } catch (NumberFormatException e) {
        LOG.warn("Could not parse fetch size '{}'. Using default of {}.", fetch, DEFAULT_FETCH_SIZE);
        return DEFAULT_FETCH_SIZE;
      }
    }
  }

  /**
   * Parse Explore connection url string to retrieve the necessary parameters to connect to CDAP.
   */
  public static ExploreConnectionParams parseConnectionUrl(String url) {
    // URI does not accept two semicolons in a URL string, hence the substring
    URI jdbcURI = URI.create(url.substring(ExploreJDBCUtils.URI_JDBC_PREFIX.length()));
    String host = jdbcURI.getHost();
    int port = jdbcURI.getPort();
    ImmutableMultimap.Builder<ExploreConnectionParams.Info, String> builder = ImmutableMultimap.builder();

    // get the query params - javadoc for getQuery says that it decodes the query URL with UTF-8 charset.
    String query = jdbcURI.getQuery();
    if (query != null) {
      for (String entry : Splitter.on("&").split(query)) {
        // Need to do it twice because of error in guava libs Issue: 1577
        int idx = entry.indexOf('=');
        if (idx <= 0) {
          continue;
        }

        ExploreConnectionParams.Info info = ExploreConnectionParams.Info.fromStr(entry.substring(0, idx));
        if (info != null) {
          builder.putAll(info, Splitter.on(',').omitEmptyStrings().split(entry.substring(idx + 1)));
        }
      }
    }
    return new ExploreConnectionParams(host, port, builder.build());
  }
}
