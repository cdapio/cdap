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
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
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
  private static final Splitter COMMA_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

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
    private final int effectivePort;
    private final ImmutableList<InetAddress> resolvedAddresses;

    private ValidatedUrl(URL url, String normalizedHost, int effectivePort, List<InetAddress> resolvedAddresses) {
      this.url = url;
      this.normalizedHost = normalizedHost;
      this.effectivePort = effectivePort;
      this.resolvedAddresses = ImmutableList.copyOf(resolvedAddresses);
    }

    public URL getUrl() {
      return url;
    }

    /**
     * Returns the lowercase host without a trailing dot. IP literals are returned in canonical form.
     */
    public String getNormalizedHost() {
      return normalizedHost;
    }

    public int getEffectivePort() {
      return effectivePort;
    }

    /**
     * Returns the validated addresses to connect to. Empty only when DNS resolution was not required.
     */
    public ImmutableList<InetAddress> getResolvedAddresses() {
      return resolvedAddresses;
    }
  }

  /**
   * Optional restrictive allowlist of OAuth provider hostnames and wildcard subdomains.
   * When empty, all public HTTPS hosts are permitted (subject to IP and redirect checks).
   */
  public static final class AllowedHosts {
    private static final AllowedHosts ALLOW_ALL = new AllowedHosts(ImmutableSet.<String>of(),
                                                                   ImmutableSet.<String>of(), false);

    private final ImmutableSet<String> exactHosts;
    private final ImmutableSet<String> wildcardSuffixes;
    private final boolean restricted;

    private AllowedHosts(ImmutableSet<String> exactHosts, ImmutableSet<String> wildcardSuffixes,
                         boolean restricted) {
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
     * Parses a comma-separated list of exact hostnames, IPv4 addresses, bracketed IPv6 addresses, or
     * wildcard subdomains (for example {@code *.salesforce.com}). Ports are not allowed.
     *
     * @throws IllegalArgumentException if any configured pattern is malformed or overly broad
     */
    public static AllowedHosts parse(@Nullable String rawConfig) {
      if (Strings.nullToEmpty(rawConfig).trim().isEmpty()) {
        return ALLOW_ALL;
      }
      ImmutableSet.Builder<String> exact = ImmutableSet.builder();
      ImmutableSet.Builder<String> wildcards = ImmutableSet.builder();
      for (String token : COMMA_SPLITTER.split(rawConfig)) {
        addHostPattern(token, exact, wildcards);
      }
      ImmutableSet<String> exactHosts = exact.build();
      ImmutableSet<String> wildcardSuffixes = wildcards.build();
      if (exactHosts.isEmpty() && wildcardSuffixes.isEmpty()) {
        throw new IllegalArgumentException("OAuth allowed hosts configuration contains no valid host patterns");
      }
      return new AllowedHosts(exactHosts, wildcardSuffixes, true);
    }

    public boolean isRestricted() {
      return restricted;
    }

    /**
     * Returns whether {@code normalizedHost} is permitted by this allowlist.
     *
     * @param normalizedHost host as returned by {@link ValidatedUrl#getNormalizedHost()}
     */
    public boolean isAllowed(String normalizedHost) {
      if (!restricted || exactHosts.contains(normalizedHost)) {
        return true;
      }
      for (String suffix : wildcardSuffixes) {
        if (normalizedHost.endsWith(suffix) && normalizedHost.length() > suffix.length()) {
          return true;
        }
      }
      return false;
    }

    private static void addHostPattern(String token, ImmutableSet.Builder<String> exact,
                                       ImmutableSet.Builder<String> wildcards) {
      String lower = token.toLowerCase(Locale.ROOT);
      if (lower.contains("/") || lower.contains("@") || lower.contains("?") || lower.contains("#")
          || lower.contains("%") || containsWhitespace(lower)) {
        throw invalidPattern(token, "must be a bare hostname, not a URL");
      }
      if (lower.startsWith("[")) {
        if (!lower.endsWith("]") || lower.length() <= 2) {
          throw invalidPattern(token, "malformed IPv6 address");
        }
        exact.add(parseIpv6Literal(lower.substring(1, lower.length() - 1)).getHostAddress());
        return;
      }
      if (lower.contains("]") || lower.contains(":")) {
        throw invalidPattern(token, "ports are not allowed and IPv6 addresses must use [brackets]");
      }
      String host = lower.endsWith(".") ? lower.substring(0, lower.length() - 1) : lower;
      if (host.startsWith("*.")) {
        String suffix = host.substring(2);
        if (!suffix.contains(".") || !isValidDnsHostname(suffix) || looksLikeIpv4(suffix)) {
          throw invalidPattern(token, "expected '*.domain.tld' with at least two domain labels");
        }
        wildcards.add("." + suffix);
        return;
      }
      if (host.contains("*")) {
        throw invalidPattern(token, "wildcards are only supported as a leading '*.' prefix");
      }
      if (looksLikeIpv4(host)) {
        byte[] ipv4 = tryParseStrictIpv4(host);
        if (ipv4 == null) {
          throw invalidPattern(token, "IPv4 addresses must use four decimal parts without leading zeros");
        }
        exact.add(host);
        return;
      }
      if (!isValidDnsHostname(host)) {
        throw invalidPattern(token, "not a valid hostname");
      }
      exact.add(host);
    }

    private static IllegalArgumentException invalidPattern(String token, String reason) {
      return new IllegalArgumentException("Invalid host pattern in OAuth allowlist: '" + token + "' (" + reason + ")");
    }
  }

  /**
   * Validates the given URL against scheme, syntax, optional host allowlist, and resolved-IP rules.
   *
   * <p>IP literals are always validated without DNS. Hostnames are resolved, and every resolved address
   * must pass the IP rules.
   *
   * @throws MalformedURLException if the URL cannot be parsed
   * @throws UnknownHostException if {@code resolution} is {@link HostResolution#REQUIRE_RESOLVABLE} and the
   *     hostname does not resolve. This is a transient infrastructure error, not invalid input.
   * @throws IllegalArgumentException if the URL violates security or allowlist policy
   */
  public ValidatedUrl validate(String rawUrl, HostResolution resolution, AllowedHosts allowedHosts)
      throws MalformedURLException, UnknownHostException {
    if (Strings.isNullOrEmpty(rawUrl)) {
      throw new IllegalArgumentException("OAuth URL must not be empty");
    }
    URL url = new URL(rawUrl);
    URI uri = toUri(url, rawUrl);

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
    String host = normalizeHost(stripIpv6Brackets(rawHost));
    if (host == null) {
      throw new IllegalArgumentException("Invalid OAuth URL host: " + rawHost);
    }

    int port = url.getPort();
    if (port == 0 || port < -1 || port > 65535) {
      throw new IllegalArgumentException("Invalid OAuth URL port: " + port);
    }
    int effectivePort = port != -1 ? port : ("https".equals(scheme) ? HTTPS_DEFAULT_PORT : HTTP_DEFAULT_PORT);

    InetAddress literal = parseIpLiteral(host);
    String normalizedHost = literal != null ? literal.getHostAddress() : host;
    if (!allowedHosts.isAllowed(normalizedHost)) {
      throw new IllegalArgumentException("OAuth URL host is not in the configured allowlist: " + normalizedHost);
    }
    if (literal != null) {
      validateInetAddress(literal, allowLoopback);
      return new ValidatedUrl(url, normalizedHost, effectivePort, ImmutableList.of(literal));
    }
    if (!allowLoopback && ("localhost".equals(host) || host.endsWith(".localhost"))) {
      throw new IllegalArgumentException("Loopback OAuth URL host is not allowed: " + host);
    }
    return new ValidatedUrl(url, normalizedHost, effectivePort, resolveAndValidate(host, rawHost, resolution));
  }

  /**
   * Checks whether two URLs share the same origin (scheme, normalized host, and effective port, with no
   * userinfo). Delegates to {@link UrlOrigin} so credential reuse and identity URL checks share one rule.
   */
  public static boolean hasSameOrigin(@Nullable String firstUrl, @Nullable String secondUrl) {
    return UrlOrigin.hasSameOrigin(firstUrl, secondUrl);
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

  private List<InetAddress> resolveAndValidate(String host, String rawHost, HostResolution resolution)
      throws UnknownHostException {
    InetAddress[] resolved;
    try {
      resolved = dnsResolver.resolve(host);
    } catch (UnknownHostException e) {
      if (resolution == HostResolution.REQUIRE_RESOLVABLE) {
        throw e;
      }
      return ImmutableList.of();
    }
    if (resolved == null || resolved.length == 0) {
      if (resolution == HostResolution.REQUIRE_RESOLVABLE) {
        throw new UnknownHostException("Unresolvable OAuth URL host: " + rawHost);
      }
      return ImmutableList.of();
    }
    for (InetAddress address : resolved) {
      validateInetAddress(address, allowLoopback);
    }
    return Arrays.asList(resolved);
  }

  private static URI toUri(URL url, String rawUrl) {
    try {
      return url.toURI();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid OAuth URL: " + rawUrl, e);
    }
  }

  /**
   * Returns the address for an IP literal host, or {@code null} for a DNS hostname.
   *
   * <p>A host whose last label is numeric is treated as IPv4, the same way browsers do. It must then be
   * four decimal parts without leading zeros. Otherwise the JDK (decimal) and browsers (octal or hex) read
   * forms such as {@code 0177.0.0.1} or {@code 2130706433} as different addresses.
   */
  @Nullable
  private static InetAddress parseIpLiteral(String host) {
    if (host.indexOf(':') >= 0) {
      return parseIpv6Literal(host);
    }
    if (!looksLikeIpv4(host)) {
      return null;
    }
    byte[] ipv4 = tryParseStrictIpv4(host);
    if (ipv4 == null) {
      throw new IllegalArgumentException(
          "OAuth URL IPv4 host must use four decimal parts without leading zeros: " + host);
    }
    try {
      return InetAddress.getByAddress(ipv4);
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("Invalid OAuth URL IPv4 host: " + host, e);
    }
  }

  /**
   * Parses an unbracketed IPv6 literal without DNS. An embedded dotted IPv4 suffix must be strict decimal.
   */
  private static InetAddress parseIpv6Literal(String host) {
    int lastColon = host.lastIndexOf(':');
    if (host.indexOf('.', lastColon + 1) >= 0 && tryParseStrictIpv4(host.substring(lastColon + 1)) == null) {
      throw new IllegalArgumentException("Invalid IPv6 address: " + host);
    }
    try {
      // Brackets make the JDK parse the text as an IPv6 literal and never fall back to DNS.
      return InetAddress.getByName("[" + host + "]");
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("Invalid IPv6 address: " + host, e);
    }
  }

  /**
   * Returns whether the last label of {@code host} is a number (decimal digits or 0x hex), which makes
   * browsers parse the whole host as an IPv4 address.
   */
  private static boolean looksLikeIpv4(String host) {
    String last = host.substring(host.lastIndexOf('.') + 1);
    if (last.isEmpty()) {
      return false;
    }
    if (last.startsWith("0x")) {
      return true;
    }
    for (int i = 0; i < last.length(); i++) {
      if (last.charAt(i) < '0' || last.charAt(i) > '9') {
        return false;
      }
    }
    return true;
  }

  @Nullable
  private static byte[] tryParseStrictIpv4(String host) {
    String[] parts = host.split("\\.", -1);
    if (parts.length != 4) {
      return null;
    }
    byte[] bytes = new byte[4];
    for (int i = 0; i < 4; i++) {
      String part = parts[i];
      if (part.isEmpty() || part.length() > 3 || (part.length() > 1 && part.charAt(0) == '0')) {
        return null;
      }
      int value = 0;
      for (int j = 0; j < part.length(); j++) {
        char c = part.charAt(j);
        if (c < '0' || c > '9') {
          return null;
        }
        value = value * 10 + (c - '0');
      }
      if (value > 255) {
        return null;
      }
      bytes[i] = (byte) value;
    }
    return bytes;
  }

  private static boolean isValidDnsHostname(String host) {
    if (host.isEmpty() || host.length() > 253 || host.startsWith(".") || host.endsWith(".")) {
      return false;
    }
    for (String label : host.split("\\.", -1)) {
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

  private static void validateEmbeddedIpv4Addresses(InetAddress address, byte[] bytes, boolean allowLoopback) {
    // IPv4-mapped (::ffff:0:0/96), IPv4-translated / SIIT (::ffff:0:0:0/96),
    // well-known NAT64 (64:ff9b::/96), or IPv4-compatible (::x.x.x.x/96, excluding :: and ::1)
    boolean isMapped = isZeroRange(bytes, 0, 10) && bytes[10] == (byte) 0xFF && bytes[11] == (byte) 0xFF;
    boolean isTranslated = isZeroRange(bytes, 0, 8) && bytes[8] == (byte) 0xFF && bytes[9] == (byte) 0xFF
        && bytes[10] == 0 && bytes[11] == 0;
    boolean isWellKnownNat64 = bytes[0] == 0 && bytes[1] == 0x64 && bytes[2] == (byte) 0xFF
        && bytes[3] == (byte) 0x9B && isZeroRange(bytes, 4, 12);
    boolean isIpv4Compatible = isZeroRange(bytes, 0, 12)
        && !address.isLoopbackAddress() && !address.isAnyLocalAddress();
    if (isMapped || isTranslated || isWellKnownNat64 || isIpv4Compatible) {
      validateEmbeddedIpv4Bytes(Arrays.copyOfRange(bytes, 12, 16), allowLoopback);
    }

    // 6to4 (2002::/16): embedded IPv4 in bytes 2..5
    if (bytes[0] == 0x20 && bytes[1] == 0x02) {
      validateEmbeddedIpv4Bytes(Arrays.copyOfRange(bytes, 2, 6), allowLoopback);
    }

    // Teredo (2001:0000::/32): server IPv4 in bytes 4..7 and bitwise-inverted client IPv4 in bytes 12..15
    if (bytes[0] == 0x20 && bytes[1] == 0x01 && bytes[2] == 0 && bytes[3] == 0) {
      validateEmbeddedIpv4Bytes(Arrays.copyOfRange(bytes, 4, 8), allowLoopback);
      byte[] clientIpv4 = new byte[] {
          (byte) ~bytes[12], (byte) ~bytes[13], (byte) ~bytes[14], (byte) ~bytes[15]
      };
      validateEmbeddedIpv4Bytes(clientIpv4, allowLoopback);
    }

    // ISATAP (RFC 5214): interface identifier 0000:5efe:w.x.y.z or 0200:5efe:w.x.y.z
    if ((bytes[8] & 0xFC) == 0 && bytes[9] == 0 && bytes[10] == 0x5E && bytes[11] == (byte) 0xFE) {
      validateEmbeddedIpv4Bytes(Arrays.copyOfRange(bytes, 12, 16), allowLoopback);
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
}
