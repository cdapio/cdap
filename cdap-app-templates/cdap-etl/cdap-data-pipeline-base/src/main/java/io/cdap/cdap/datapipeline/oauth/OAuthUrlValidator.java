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

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Validates outbound Pipeline Studio OAuth URLs against scheme, syntax, optional host allowlist,
 * and resolved-IP SSRF rules.
 */
public final class OAuthUrlValidator {
  private static final int HTTP_DEFAULT_PORT = 80;
  private static final int HTTPS_DEFAULT_PORT = 443;
  private static final Pattern HOST_LABEL_PATTERN = Pattern.compile("^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$");

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
   * Creates a validator for unit and integration tests with configurable scheme, loopback handling,
   * and DNS resolution. All non-loopback private, link-local, and reserved IP ranges remain blocked.
   */
  @VisibleForTesting
  public static OAuthUrlValidator forTesting(boolean requireHttps, boolean allowLoopback, DnsResolver dnsResolver) {
    return new OAuthUrlValidator(requireHttps, allowLoopback, dnsResolver);
  }

  /**
   * Controls whether an unresolvable DNS hostname is permitted during validation.
   */
  public enum HostResolution {
    /** Allow unresolvable DNS hostnames (used during provider registration); IP literals are still validated. */
    ALLOW_UNRESOLVED,
    /** Require DNS resolution to succeed with at least one valid IP address (used before outbound calls). */
    REQUIRE_RESOLVABLE
  }

  /**
   * Abstraction over DNS resolution so tests can inject deterministic records and simulate rebinding.
   */
  @FunctionalInterface
  public interface DnsResolver {
    InetAddress[] resolve(String host) throws UnknownHostException;
  }

  /**
   * Immutable result of validating an OAuth URL, carrying the resolved IP addresses to pin connections to.
   */
  public static final class ValidatedUrl {
    private final URL url;
    private final String normalizedHost;
    private final String connectHost;
    private final int effectivePort;
    private final List<InetAddress> resolvedAddresses;

    private ValidatedUrl(URL url, String normalizedHost, String connectHost, int effectivePort,
                         List<InetAddress> resolvedAddresses) {
      this.url = url;
      this.normalizedHost = normalizedHost;
      this.connectHost = connectHost;
      this.effectivePort = effectivePort;
      this.resolvedAddresses = Collections.unmodifiableList(new ArrayList<>(resolvedAddresses));
    }

    public URL getUrl() {
      return url;
    }

    public String getNormalizedHost() {
      return normalizedHost;
    }

    public String getConnectHost() {
      return connectHost;
    }

    public int getEffectivePort() {
      return effectivePort;
    }

    public List<InetAddress> getResolvedAddresses() {
      return resolvedAddresses;
    }
  }

  /**
   * Optional restrictive allowlist of OAuth provider hostnames and wildcard subdomains.
   * When empty, all public HTTPS hosts are permitted (subject to IP and redirect checks).
   */
  public static final class AllowedHosts {
    private static final AllowedHosts ALLOW_ALL =
        new AllowedHosts(Collections.emptySet(), Collections.emptyList(), false);

    private final Set<String> exactHosts;
    private final List<String> wildcardSuffixes;
    private final boolean restricted;

    private AllowedHosts(Set<String> exactHosts, List<String> wildcardSuffixes, boolean restricted) {
      this.exactHosts = exactHosts;
      this.wildcardSuffixes = wildcardSuffixes;
      this.restricted = restricted;
    }

    /**
     * Returns an unrestricted allowlist that permits any public host passing IP checks.
     */
    public static AllowedHosts allowAll() {
      return ALLOW_ALL;
    }

    /**
     * Parses a comma-separated list of exact hostnames or wildcard subdomains (e.g. {@code *.salesforce.com}).
     *
     * @throws IllegalArgumentException if any configured pattern is malformed or overly broad
     */
    public static AllowedHosts parse(@Nullable String rawConfig) {
      if (rawConfig == null || rawConfig.trim().isEmpty()) {
        return ALLOW_ALL;
      }
      Set<String> exact = new HashSet<>();
      List<String> wildcards = new ArrayList<>();
      for (String rawToken : rawConfig.split(",")) {
        String token = rawToken.trim();
        if (token.isEmpty()) {
          continue;
        }
        String normalized = normalizeConfiguredPattern(token);
        if (normalized.startsWith("*.")) {
          String suffix = normalized.substring(2);
          if (!suffix.contains(".") || !isValidDnsHostname(suffix)) {
            throw new IllegalArgumentException(
                "Invalid wildcard host pattern in OAuth allowlist: '" + token
                    + "'. Expected '*.domain.tld' with at least two domain labels.");
          }
          wildcards.add("." + suffix);
        } else {
          if (normalized.contains("*")) {
            throw new IllegalArgumentException(
                "Invalid wildcard host pattern in OAuth allowlist: '" + token
                    + "'. Wildcards are only supported as a leading '*.' prefix.");
          }
          if (!isValidExactHostPattern(normalized)) {
            throw new IllegalArgumentException(
                "Invalid host pattern in OAuth allowlist: '" + token + "'");
          }
          exact.add(normalized);
        }
      }
      if (exact.isEmpty() && wildcards.isEmpty()) {
        throw new IllegalArgumentException("OAuth allowed hosts configuration contains no valid host patterns");
      }
      return new AllowedHosts(Collections.unmodifiableSet(exact), Collections.unmodifiableList(wildcards), true);
    }

    public boolean isRestricted() {
      return restricted;
    }

    public boolean isAllowed(String normalizedHost) {
      if (!restricted) {
        return true;
      }
      if (exactHosts.contains(normalizedHost)) {
        return true;
      }
      for (String suffix : wildcardSuffixes) {
        if (normalizedHost.endsWith(suffix) && normalizedHost.length() > suffix.length()) {
          return true;
        }
      }
      return false;
    }

    private static String normalizeConfiguredPattern(String token) {
      if (token.contains("://") || token.contains("/") || token.contains("@")
          || token.contains("?") || token.contains("#") || token.contains("%")
          || containsWhitespace(token)) {
        throw new IllegalArgumentException(
            "Invalid host pattern in OAuth allowlist (must be a bare hostname, not a URL): '" + token + "'");
      }
      String unbracketed = stripIpv6Brackets(token);
      if (token.startsWith("[") || token.endsWith("]")) {
        if (!(token.startsWith("[") && token.endsWith("]")) || unbracketed.isEmpty()) {
          throw new IllegalArgumentException("Invalid IPv6 host pattern in OAuth allowlist: '" + token + "'");
        }
      }
      String lower = unbracketed.toLowerCase(Locale.ROOT);
      if (lower.endsWith(".")) {
        lower = lower.substring(0, lower.length() - 1);
      }
      if (lower.isEmpty() || lower.endsWith(".")) {
        throw new IllegalArgumentException("Invalid host pattern in OAuth allowlist: '" + token + "'");
      }
      return lower;
    }

    private static boolean isValidExactHostPattern(String host) {
      if (host.contains(":")) {
        // IPv6 literal: allow hex digits, colons, and dots (for embedded IPv4 form)
        for (int i = 0; i < host.length(); i++) {
          char c = host.charAt(i);
          boolean ok = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || c == ':' || c == '.';
          if (!ok) {
            return false;
          }
        }
        return true;
      }
      return isValidDnsHostname(host);
    }

    private static boolean isValidDnsHostname(String host) {
      if (host.isEmpty() || host.length() > 253 || host.startsWith(".") || host.endsWith(".")) {
        return false;
      }
      String[] labels = host.split("\\.", -1);
      for (String label : labels) {
        if (!HOST_LABEL_PATTERN.matcher(label).matches()) {
          return false;
        }
      }
      return true;
    }

    private static boolean containsWhitespace(String value) {
      for (int i = 0; i < value.length(); i++) {
        if (Character.isWhitespace(value.charAt(i))) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Validates the given URL against scheme, syntax, optional host allowlist, and resolved-IP rules.
   *
   * @throws MalformedURLException if the URL cannot be parsed
   * @throws IllegalArgumentException if the URL violates security or allowlist policy
   */
  public ValidatedUrl validate(String rawUrl, HostResolution resolution, AllowedHosts allowedHosts)
      throws MalformedURLException {
    if (Strings.isNullOrEmpty(rawUrl)) {
      throw new IllegalArgumentException("OAuth URL must not be empty");
    }
    URL url = new URL(rawUrl);
    URI uri;
    try {
      uri = url.toURI();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid OAuth URL: " + rawUrl, e);
    }

    String scheme = url.getProtocol().toLowerCase(Locale.ROOT);
    if (requireHttps) {
      if (!"https".equals(scheme)) {
        throw new IllegalArgumentException("OAuth URL must use https scheme: " + scheme);
      }
    } else if (!"https".equals(scheme) && !"http".equals(scheme)) {
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

    String connectHost = stripIpv6Brackets(rawHost);
    String normalizedHost = normalizeHost(connectHost);
    if (normalizedHost == null) {
      throw new IllegalArgumentException("Invalid OAuth URL host: " + rawHost);
    }

    int port = url.getPort();
    if (port == 0 || port < -1 || port > 65535) {
      throw new IllegalArgumentException("Invalid OAuth URL port: " + port);
    }
    int effectivePort = port != -1 ? port : ("https".equals(scheme) ? HTTPS_DEFAULT_PORT : HTTP_DEFAULT_PORT);

    if (!allowedHosts.isAllowed(normalizedHost)) {
      throw new IllegalArgumentException("OAuth URL host is not in the configured allowlist: " + normalizedHost);
    }

    InetAddress[] resolved;
    try {
      resolved = dnsResolver.resolve(normalizedHost);
    } catch (UnknownHostException e) {
      if (resolution == HostResolution.REQUIRE_RESOLVABLE) {
        throw new IllegalArgumentException("Unresolvable OAuth URL host: " + rawHost, e);
      }
      return new ValidatedUrl(url, normalizedHost, normalizedHost, effectivePort, Collections.emptyList());
    }

    if (resolved == null || resolved.length == 0) {
      if (resolution == HostResolution.REQUIRE_RESOLVABLE) {
        throw new IllegalArgumentException("Unresolvable OAuth URL host: " + rawHost);
      }
      return new ValidatedUrl(url, normalizedHost, normalizedHost, effectivePort, Collections.emptyList());
    }

    for (InetAddress address : resolved) {
      validateInetAddress(address, allowLoopback);
    }
    return new ValidatedUrl(url, normalizedHost, normalizedHost, effectivePort, Arrays.asList(resolved));
  }

  /**
   * Checks whether two URLs share the same origin (scheme, normalized host stripping a single trailing dot,
   * and effective port, with no userinfo).
   */
  public static boolean hasSameOrigin(@Nullable String firstUrl, @Nullable String secondUrl) {
    if (Strings.isNullOrEmpty(firstUrl) || Strings.isNullOrEmpty(secondUrl)) {
      return false;
    }
    String firstOrigin = extractNormalizedOrigin(firstUrl);
    String secondOrigin = extractNormalizedOrigin(secondUrl);
    return firstOrigin != null && firstOrigin.equals(secondOrigin);
  }

  /**
   * Validates that the given {@link InetAddress} is a public, routable address (or loopback when explicitly
   * enabled for tests), including unwrapping IPv4 addresses embedded inside IPv6 transition prefixes.
   */
  @VisibleForTesting
  public static void validateInetAddress(InetAddress address, boolean allowLoopback) {
    byte[] bytes = address.getAddress();
    boolean isZeroNet = bytes.length == 4 && (bytes[0] & 0xFF) == 0;
    boolean isReservedOrBroadcast = bytes.length == 4 && (bytes[0] & 0xF0) == 0xF0;
    boolean isCgnat = bytes.length == 4 && (bytes[0] & 0xFF) == 100 && (bytes[1] & 0xC0) == 64;
    boolean isBenchmark = bytes.length == 4 && (bytes[0] & 0xFF) == 198 && (bytes[1] & 0xFE) == 18;
    boolean isUla = bytes.length == 16 && (bytes[0] & 0xFE) == 0xFC;
    boolean isLocalNat64 = bytes.length == 16
        && bytes[0] == 0 && bytes[1] == 0x64 && bytes[2] == (byte) 0xFF && bytes[3] == (byte) 0x9B
        && bytes[4] == 0 && bytes[5] == 1;
    boolean isIpv6Discard = bytes.length == 16
        && bytes[0] == 0x01 && bytes[1] == 0 && isZeroRange(bytes, 2, 8);

    if (address.isAnyLocalAddress() || address.isLinkLocalAddress() || address.isMulticastAddress()
        || address.isSiteLocalAddress() || isZeroNet || isReservedOrBroadcast
        || isCgnat || isBenchmark || isUla || isLocalNat64 || isIpv6Discard) {
      throw new IllegalArgumentException("Disallowed OAuth URL IP address: " + address.getHostAddress());
    }

    if (!allowLoopback && address.isLoopbackAddress()) {
      throw new IllegalArgumentException("Loopback OAuth URL IP address is not allowed: " + address.getHostAddress());
    }

    if (address instanceof Inet6Address && bytes.length == 16) {
      validateEmbeddedIpv4Addresses(address, bytes, allowLoopback);
    }
  }

  private static void validateEmbeddedIpv4Addresses(InetAddress address, byte[] b, boolean allowLoopback) {
    // IPv4-mapped (::ffff:0:0/96), IPv4-translated / SIIT (::ffff:0:0:0/96),
    // well-known NAT64 (64:ff9b::/96), or IPv4-compatible (::x.x.x.x/96, excluding :: and ::1)
    boolean isMapped = isZeroRange(b, 0, 10) && b[10] == (byte) 0xFF && b[11] == (byte) 0xFF;
    boolean isTranslated = isZeroRange(b, 0, 8) && b[8] == (byte) 0xFF && b[9] == (byte) 0xFF
        && b[10] == 0 && b[11] == 0;
    boolean isWellKnownNat64 = b[0] == 0 && b[1] == 0x64 && b[2] == (byte) 0xFF && b[3] == (byte) 0x9B
        && isZeroRange(b, 4, 12);
    boolean isIpv4Compatible = isZeroRange(b, 0, 12)
        && !address.isLoopbackAddress() && !address.isAnyLocalAddress();
    if (isMapped || isTranslated || isWellKnownNat64 || isIpv4Compatible) {
      validateEmbeddedIpv4Bytes(Arrays.copyOfRange(b, 12, 16), allowLoopback);
    }

    // 6to4 (2002::/16): embedded IPv4 in bytes 2..5
    if (b[0] == 0x20 && b[1] == 0x02) {
      validateEmbeddedIpv4Bytes(Arrays.copyOfRange(b, 2, 6), allowLoopback);
    }

    // Teredo (2001:0000::/32): server IPv4 in bytes 4..7 and bitwise-inverted client IPv4 in bytes 12..15
    if (b[0] == 0x20 && b[1] == 0x01 && b[2] == 0 && b[3] == 0) {
      validateEmbeddedIpv4Bytes(Arrays.copyOfRange(b, 4, 8), allowLoopback);
      byte[] clientIpv4 = new byte[] {
          (byte) ~b[12], (byte) ~b[13], (byte) ~b[14], (byte) ~b[15]
      };
      validateEmbeddedIpv4Bytes(clientIpv4, allowLoopback);
    }

    // ISATAP (RFC 5214): interface identifier 0000:5efe:w.x.y.z or 0200:5efe:w.x.y.z
    if ((b[8] & 0xFC) == 0 && b[9] == 0 && b[10] == 0x5E && b[11] == (byte) 0xFE) {
      validateEmbeddedIpv4Bytes(Arrays.copyOfRange(b, 12, 16), allowLoopback);
    }
  }

  private static void validateEmbeddedIpv4Bytes(byte[] ipv4Bytes, boolean allowLoopback) {
    try {
      validateInetAddress(InetAddress.getByAddress(ipv4Bytes), allowLoopback);
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("Invalid embedded IPv4 address", e);
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
    if (host.startsWith("[") && host.endsWith("]") && host.length() > 2) {
      return host.substring(1, host.length() - 1);
    }
    return host;
  }

  @Nullable
  private static String normalizeHost(@Nullable String host) {
    if (Strings.isNullOrEmpty(host)) {
      return null;
    }
    String normalized = host.endsWith(".") ? host.substring(0, host.length() - 1) : host;
    if (normalized.isEmpty() || normalized.endsWith(".")) {
      return null;
    }
    return normalized.toLowerCase(Locale.ROOT);
  }

  @Nullable
  private static String extractNormalizedOrigin(String rawUrl) {
    URI uri;
    try {
      uri = URI.create(rawUrl);
    } catch (IllegalArgumentException e) {
      return null;
    }
    if (uri.getRawUserInfo() != null) {
      return null;
    }
    String rawScheme = uri.getScheme();
    String rawHost = uri.getHost();
    if (rawScheme == null || rawHost == null || rawHost.contains("%")) {
      return null;
    }
    String scheme = rawScheme.toLowerCase(Locale.ROOT);
    String host = normalizeHost(stripIpv6Brackets(rawHost));
    if (host == null) {
      return null;
    }
    int port = uri.getPort();
    if (port == -1) {
      if ("http".equals(scheme)) {
        port = HTTP_DEFAULT_PORT;
      } else if ("https".equals(scheme)) {
        port = HTTPS_DEFAULT_PORT;
      }
    }
    return scheme + "://" + host + ":" + port;
  }
}
