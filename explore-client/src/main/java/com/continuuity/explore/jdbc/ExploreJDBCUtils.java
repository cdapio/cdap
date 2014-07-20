/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.explore.jdbc;

import com.google.common.collect.ImmutableMap;

import java.net.URI;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility methods and constants to use in Explore JDBC driver.
 */
public class ExploreJDBCUtils {

  public static final String URI_JDBC_PREFIX = "jdbc:";

  /**
   * Explore connection parameters.
   */
  public static final class ConnectionParams {

    /**
     * Extra Explore connection parameter.
     */
    public enum Info {
      EXPLORE_AUTH_TOKEN("reactor.auth.token");

      private String name;

      private Info(String name) {
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
    private final Map<Info, String> extraInfos;

    private ConnectionParams(String host, int port, Map<Info, String> extraInfos) {
      this.host = host;
      this.port = port;
      this.extraInfos = extraInfos;
    }

    public Map<Info, String> getExtraInfos() {
      return extraInfos;
    }

    public int getPort() {
      return port;
    }

    public String getHost() {
      return host;
    }
  }

  private static final Pattern CONNECTION_PARAMS_PATTERN = Pattern.compile("([^;]*)=([^;]*)[;]?");

  /**
   * Parse Explore connection url string to retrieve the necessary parameters to connect to Reactor.
   */
  static ConnectionParams parseConnectionUrl(String url) {
    URI jdbcURI = URI.create(url.substring(ExploreJDBCUtils.URI_JDBC_PREFIX.length()));
    String host = jdbcURI.getHost();
    int port = jdbcURI.getPort();

    String query = jdbcURI.getQuery();
    ImmutableMap.Builder<ConnectionParams.Info, String> builder = ImmutableMap.builder();
    if (query != null) {
      Matcher matcher = CONNECTION_PARAMS_PATTERN.matcher(query);
      while (matcher.find()) {
        ConnectionParams.Info info = ConnectionParams.Info.fromStr(matcher.group(1));
        if (info != null) {
          builder.put(info, matcher.group(2));
        }
      }
    }
    return new ConnectionParams(host, port, builder.build());
  }

}
