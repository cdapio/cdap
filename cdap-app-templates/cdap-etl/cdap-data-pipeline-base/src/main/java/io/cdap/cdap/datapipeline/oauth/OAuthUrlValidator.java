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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.net.InetAddresses;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Validates Pipeline Studio OAuth URLs against scheme, syntax, and resolved-IP SSRF rules.
 */
public final class OAuthUrlValidator {
  private static final Pattern NUMERIC_LABEL_PATTERN = Pattern.compile("^(?:\\d+|0x[0-9a-f]+)$");

  private final boolean requireHttps;
  private final boolean allowLoopback;
  private final DnsResolver dnsResolver;

  private OAuthUrlValidator(boolean requireHttps, boolean allowLoopback, DnsResolver dnsResolver) {
    this.requireHttps = requireHttps;
    this.allowLoopback = allowLoopback;
    this.dnsResolver = Objects.requireNonNull(dnsResolver, "dnsResolver must not be null");
  }

  /**
   * Returns the strict production validator: requires HTTPS and blocks all non-public IP ranges.
   */
  public static OAuthUrlValidator strict() {
    return new OAuthUrlValidator(true, false, InetAddress::getAllByName);
  }

  /**
   * Creates a validator for tests with configurable scheme, loopback handling, and DNS resolution.
   * Non-loopback private, link-local, and reserved IP ranges always remain blocked.
   */
  @VisibleForTesting
  public static OAuthUrlValidator forTesting(boolean requireHttps, boolean allowLoopback, DnsResolver dnsResolver) {
    return new OAuthUrlValidator(requireHttps, allowLoopback, dnsResolver);
  }

  /**
   * Abstraction over DNS resolution so tests can inject deterministic records.
   */
  @FunctionalInterface
  public interface DnsResolver {
    InetAddress[] resolve(String host) throws UnknownHostException;
  }

  /**
   * Validates {@code rawUrl} against scheme, syntax, and resolved-IP rules.
   *
   * @throws MalformedURLException if the URL cannot be parsed
   * @throws UnknownHostException if {@code requireResolvable} is true and DNS resolution fails
   * @throws IllegalArgumentException if the URL violates scheme, syntax, or IP rules
   */
  public URL validate(String rawUrl, boolean requireResolvable) throws MalformedURLException, UnknownHostException {
    if (Strings.isNullOrEmpty(rawUrl)) {
      throw new IllegalArgumentException("OAuth URL must not be empty");
    }
    return validate(new URL(rawUrl), requireResolvable);
  }

  /**
   * Validates a parsed {@link URL} against scheme, syntax, and resolved-IP rules.
   *
   * @throws UnknownHostException if {@code requireResolvable} is true and DNS resolution fails
   * @throws IllegalArgumentException if the URL violates scheme, syntax, or IP rules
   */
  public URL validate(URL url, boolean requireResolvable) throws UnknownHostException {
    URI uri;
    try {
      uri = url.toURI();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid OAuth URL: " + url, e);
    }

    String scheme = url.getProtocol().toLowerCase(Locale.ROOT);
    if (requireHttps && !"https".equals(scheme)) {
      throw new IllegalArgumentException("OAuth URL must use https scheme: " + scheme);
    }
    if (!"https".equals(scheme) && !"http".equals(scheme)) {
      throw new IllegalArgumentException("Invalid OAuth URL scheme: " + scheme);
    }
    if (url.getUserInfo() != null || uri.getRawUserInfo() != null) {
      throw new IllegalArgumentException("OAuth URL must not contain userinfo");
    }

    String rawHost = url.getHost();
    if (Strings.isNullOrEmpty(rawHost) || Strings.isNullOrEmpty(uri.getHost())
        || rawHost.contains("%") || rawHost.contains("\\")) {
      throw new IllegalArgumentException("Invalid OAuth URL host: " + rawHost);
    }
    String host = stripIpv6Brackets(rawHost);
    if (host.endsWith(".")) {
      host = host.substring(0, host.length() - 1);
    }
    if (host.isEmpty() || host.endsWith(".")) {
      throw new IllegalArgumentException("Invalid OAuth URL host: " + rawHost);
    }
    host = host.toLowerCase(Locale.ROOT);

    int port = url.getPort();
    if (port == 0 || port < -1 || port > 65535) {
      throw new IllegalArgumentException("Invalid OAuth URL port: " + port);
    }

    if (InetAddresses.isInetAddress(host)) {
      validateInetAddress(InetAddresses.forString(host), allowLoopback);
      return url;
    }
    if (host.indexOf(':') >= 0) {
      throw new IllegalArgumentException("Invalid IPv6 address: " + host);
    }
    if (NUMERIC_LABEL_PATTERN.matcher(host.substring(host.lastIndexOf('.') + 1)).matches()) {
      throw new IllegalArgumentException(
          "OAuth URL IPv4 host must use four decimal parts without leading zeros: " + host);
    }
    if (!allowLoopback && ("localhost".equals(host) || host.endsWith(".localhost"))) {
      throw new IllegalArgumentException("Loopback OAuth URL host is not allowed: " + host);
    }

    InetAddress[] resolved;
    try {
      resolved = dnsResolver.resolve(host);
    } catch (UnknownHostException e) {
      if (requireResolvable) {
        throw e;
      }
      return url;
    }
    if (resolved == null || resolved.length == 0) {
      if (requireResolvable) {
        throw new UnknownHostException("Unresolvable OAuth URL host: " + rawHost);
      }
      return url;
    }
    for (InetAddress address : resolved) {
      validateInetAddress(address, allowLoopback);
    }
    return url;
  }

  /**
   * Validates that {@code address} is a public, routable IP (or loopback when enabled for tests),
   * including unwrapping IPv4 addresses embedded inside IPv6 transition prefixes.
   */
  @VisibleForTesting
  public static void validateInetAddress(InetAddress address, boolean allowLoopback) {
    byte[] bytes = address.getAddress();
    boolean blockedIpv4 = bytes.length == 4 && isBlockedIpv4(bytes);
    boolean blockedIpv6 = bytes.length == 16 && isBlockedIpv6Prefix(bytes);

    if (address.isAnyLocalAddress() || address.isLinkLocalAddress() || address.isMulticastAddress()
        || address.isSiteLocalAddress() || blockedIpv4 || blockedIpv6) {
      throw new IllegalArgumentException("Disallowed OAuth URL IP address: " + address.getHostAddress());
    }
    if (!allowLoopback && address.isLoopbackAddress()) {
      throw new IllegalArgumentException("Loopback OAuth URL IP address is not allowed: " + address.getHostAddress());
    }
    if (address instanceof Inet6Address && bytes.length == 16) {
      validateEmbeddedIpv4((Inet6Address) address, bytes, allowLoopback);
    }
  }

  private static boolean isBlockedIpv4(byte[] bytes) {
    int b0 = bytes[0] & 0xFF;
    int b1 = bytes[1] & 0xFF;
    return b0 == 0                                      // 0.0.0.0/8 ("this network")
        || (b0 == 100 && (b1 & 0xC0) == 64)             // 100.64.0.0/10 (CGNAT)
        || (b0 == 198 && (b1 & 0xFE) == 18)             // 198.18.0.0/15 (benchmarking)
        || (b0 & 0xF0) == 0xF0;                         // 240.0.0.0/4 (reserved / broadcast)
  }

  private static boolean isBlockedIpv6Prefix(byte[] bytes) {
    boolean isUla = (bytes[0] & 0xFE) == 0xFC;
    boolean isLocalNat64 = bytes[0] == 0 && bytes[1] == 0x64 && bytes[2] == (byte) 0xFF
        && bytes[3] == (byte) 0x9B && bytes[4] == 0 && bytes[5] == 1;
    boolean isDiscard = bytes[0] == 0x01 && bytes[1] == 0 && isZeroRange(bytes, 2, 8);
    return isUla || isLocalNat64 || isDiscard;
  }

  private static void validateEmbeddedIpv4(Inet6Address address, byte[] bytes, boolean allowLoopback) {
    if (InetAddresses.hasEmbeddedIPv4ClientAddress(address)) {
      validateInetAddress(InetAddresses.getEmbeddedIPv4ClientAddress(address), allowLoopback);
    }
    if (InetAddresses.isTeredoAddress(address)) {
      validateInetAddress(InetAddresses.getTeredoInfo(address).getServer(), allowLoopback);
    }
    if (InetAddresses.isIsatapAddress(address)) {
      validateInetAddress(InetAddresses.getIsatapIPv4Address(address), allowLoopback);
    }
    boolean isMapped = isZeroRange(bytes, 0, 10) && bytes[10] == (byte) 0xFF && bytes[11] == (byte) 0xFF;
    boolean isSiit = isZeroRange(bytes, 0, 8) && bytes[8] == (byte) 0xFF && bytes[9] == (byte) 0xFF
        && bytes[10] == 0 && bytes[11] == 0;
    boolean isNat64 = bytes[0] == 0 && bytes[1] == 0x64 && bytes[2] == (byte) 0xFF && bytes[3] == (byte) 0x9B
        && isZeroRange(bytes, 4, 12);
    if (isMapped || isSiit || isNat64) {
      try {
        validateInetAddress(InetAddress.getByAddress(Arrays.copyOfRange(bytes, 12, 16)), allowLoopback);
      } catch (UnknownHostException e) {
        throw new IllegalArgumentException("Invalid embedded IPv4 address", e);
      }
    }
  }

  private static boolean isZeroRange(byte[] bytes, int startInclusive, int endExclusive) {
    for (int i = startInclusive; i < endExclusive; i++) {
      if (bytes[i] != 0) {
        return false;
      }
    }
    return true;
  }

  private static String stripIpv6Brackets(String host) {
    return host.startsWith("[") && host.endsWith("]") && host.length() > 2
        ? host.substring(1, host.length() - 1) : host;
  }
}
