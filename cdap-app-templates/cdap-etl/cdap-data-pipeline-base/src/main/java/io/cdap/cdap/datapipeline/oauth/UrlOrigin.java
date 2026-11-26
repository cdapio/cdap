/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.datapipeline.oauth;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Locale;
import javax.annotation.Nullable;

/**
 * Package-private utility to validate OAuth provider URL origins (http or https scheme, normalized
 * host, and effective port, with no userinfo).
 */
final class UrlOrigin {
  private static final int HTTP_DEFAULT_PORT = 80;
  private static final int HTTPS_DEFAULT_PORT = 443;

  private UrlOrigin() {
  }

  static boolean isValid(@Nullable String url) {
    return hasSameOrigin(url, url);
  }

  static boolean hasSameOrigin(@Nullable String firstUrl, @Nullable String secondUrl) {
    URL first = parseValidUrl(firstUrl);
    URL second = parseValidUrl(secondUrl);
    if (first == null || second == null) {
      return false;
    }
    String firstHost = normalizeHost(first.getHost());
    String secondHost = normalizeHost(second.getHost());
    if (firstHost == null || !firstHost.equals(secondHost)) {
      return false;
    }
    String firstScheme = first.getProtocol().toLowerCase(Locale.ROOT);
    String secondScheme = second.getProtocol().toLowerCase(Locale.ROOT);
    return firstScheme.equals(secondScheme)
        && getEffectivePort(firstScheme, first.getPort())
            == getEffectivePort(secondScheme, second.getPort());
  }

  @Nullable
  private static URL parseValidUrl(@Nullable String url) {
    if (url == null || url.isEmpty() || hasUnsafeCharacters(url)) {
      return null;
    }
    URL parsed;
    try {
      parsed = new URL(url);
    } catch (MalformedURLException e) {
      return null;
    }
    String scheme = parsed.getProtocol();
    String authority = parsed.getAuthority();
    int port = parsed.getPort();
    if (scheme == null || authority == null || authority.isEmpty()
        || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))
        || !url.regionMatches(true, scheme.length(), "://", 0, 3)
        || parsed.getUserInfo() != null
        || authority.indexOf('@') != -1
        || authority.indexOf('\\') != -1
        || authority.contains(":+")
        || (port == -1 && hasPortSeparator(authority))
        || (port != -1 && (port < 1 || port > 65535))) {
      return null;
    }
    return parsed;
  }

  /**
   * Returns true for leading or trailing spaces and for control characters anywhere in the URL.
   */
  private static boolean hasUnsafeCharacters(String url) {
    if (url.charAt(0) == ' ' || url.charAt(url.length() - 1) == ' ') {
      return true;
    }
    for (int i = 0; i < url.length(); i++) {
      char c = url.charAt(i);
      if (c < ' ' || c == 0x7f) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true if the authority has a port separator, such as {@code host:} or {@code host:-1}. Called
   * only when no valid port was parsed, so a separator means the port is empty or invalid.
   */
  private static boolean hasPortSeparator(String authority) {
    return authority.lastIndexOf(':') > authority.lastIndexOf(']');
  }

  @Nullable
  private static String normalizeHost(@Nullable String host) {
    if (host == null || host.isEmpty()) {
      return null;
    }
    if (host.startsWith("[")) {
      try {
        return new URI("http://" + host).getHost() != null ? host.toLowerCase(Locale.ROOT) : null;
      } catch (URISyntaxException e) {
        return null;
      }
    }
    String normalized = host.endsWith(".") ? host.substring(0, host.length() - 1) : host;
    if (normalized.isEmpty() || normalized.startsWith(".") || normalized.endsWith(".")
        || normalized.contains("..")) {
      return null;
    }
    for (int i = 0; i < normalized.length(); i++) {
      if (!isHostCharacter(normalized.charAt(i))) {
        return null;
      }
    }
    return normalized.toLowerCase(Locale.ROOT);
  }

  /**
   * Allows ASCII letters, digits, '-', '.', and '_' (used by internal gateway hostnames). Non-ASCII
   * hostnames must use their punycode form.
   */
  private static boolean isHostCharacter(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
        || c == '-' || c == '.' || c == '_';
  }

  private static int getEffectivePort(String normalizedScheme, int port) {
    if (port != -1) {
      return port;
    }
    return normalizedScheme.equals("https") ? HTTPS_DEFAULT_PORT : HTTP_DEFAULT_PORT;
  }
}
