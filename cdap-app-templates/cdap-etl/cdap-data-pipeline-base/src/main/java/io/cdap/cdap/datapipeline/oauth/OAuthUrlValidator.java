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
import com.google.common.net.InetAddresses;

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
  private static final Pattern NUMERIC_LABEL_PATTERN = Pattern.compile("^(?:\\d+|0x[0-9a-f]+)$");
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
   * Creates a validator for tests with configurable scheme, loopback handling, and DNS resolution.
   * Non-loopback private, link-local, and reserved IP ranges always remain blocked.
   */
  @VisibleForTesting
  public static OAuthUrlValidator forTesting(boolean requireHttps, boolean allowLoopback, DnsResolver dnsResolver) {
    return new OAuthUrlValidator(requireHttps, allowLoopback, dnsResolver);
  }

  /**
   * Controls whether an unresolvable DNS hostname is permitted during validation.
   */
  public enum HostResolution {
    /** Allow unresolvable DNS hostnames (provider registration); IP literals are still validated. */
    ALLOW_UNRESOLVED,
    /** Require DNS resolution to succeed with at least one valid IP address (before outbound calls). */
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
     * Returns the lowercase host without a trailing dot, or canonical IP string for IP literals.
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
    private static final AllowedHosts ALLOW_ALL =
        new AllowedHosts(ImmutableSet.<String>of(), ImmutableSet.<String>of(), false);

    private final ImmutableSet<String> exactHosts;
    private final ImmutableSet<String> wildcardSuffixes;
    private final boolean restricted;

    private AllowedHosts(ImmutableSet<String> exactHosts, ImmutableSet<String> wildcardSuffixes,
                         boolean restricted) {
      this.exactHosts = exactHosts;
      this.wildcardSuffixes = wildcardSuffixes;
      this.restricted = restricted;
    }

    public static AllowedHosts allowAll() {
      return ALLOW_ALL;
    }

    /**
     * Parses a comma-separated list of exact hostnames or wildcard subdomains (for example
     * {@code *.salesforce.com}). Ports, URLs, and raw IP addresses are rejected.
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
      String host = lower.endsWith(".") ? lower.substring(0, lower.length() - 1) : lower;
      if (host.startsWith("*.")) {
        String suffix = host.substring(2);
        if (!suffix.contains(".") || !isValidDnsHostname(suffix) || isNumericLastLabel(suffix)) {
          throw new IllegalArgumentException(
              "Invalid host pattern in OAuth allowlist: '" + token + "' (expected '*.domain.tld')");
        }
        wildcards.add("." + suffix);
        return;
      }
      if (!isValidDnsHostname(host) || isNumericLastLabel(host)) {
        throw new IllegalArgumentException(
            "Invalid host pattern in OAuth allowlist: '" + token + "' (expected a hostname or '*.domain.tld')");
      }
      exact.add(host);
    }
  }

  /**
   * Validates the given URL against scheme, syntax, optional host allowlist, and resolved-IP rules.
   * IP literals are validated without DNS; hostnames are resolved and every returned IP must pass.
   *
   * @throws MalformedURLException if the URL cannot be parsed
   * @throws UnknownHostException if {@code resolution} is {@link HostResolution#REQUIRE_RESOLVABLE} and
   *     DNS fails (transient infrastructure error, not invalid input)
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

  @Nullable
  private static InetAddress parseIpLiteral(String host) {
    if (InetAddresses.isInetAddress(host)) {
      return InetAddresses.forString(host);
    }
    if (host.indexOf(':') >= 0) {
      throw new IllegalArgumentException("Invalid IPv6 address: " + host);
    }
    // Reject octal/hex/short/integer IPv4 forms (e.g. 0177.0.0.1, 127.1, 2130706433) without DNS.
    if (isNumericLastLabel(host)) {
      throw new IllegalArgumentException(
          "OAuth URL IPv4 host must use four decimal parts without leading zeros: " + host);
    }
    return null;
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
    // Guava extracts IPv4-compatible (::x.x.x.x), 6to4 (2002::/16), Teredo (2001:0::/32), and ISATAP.
    if (InetAddresses.hasEmbeddedIPv4ClientAddress(address)) {
      validateInetAddress(InetAddresses.getEmbeddedIPv4ClientAddress(address), allowLoopback);
    }
    if (InetAddresses.isTeredoAddress(address)) {
      validateInetAddress(InetAddresses.getTeredoInfo(address).getServer(), allowLoopback);
    }
    if (InetAddresses.isIsatapAddress(address)) {
      validateInetAddress(InetAddresses.getIsatapIPv4Address(address), allowLoopback);
    }
    // Also unwrap 16-byte IPv4-mapped (::ffff:0:0/96), SIIT (::ffff:0:0:0/96), and well-known NAT64 (64:ff9b::/96).
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

  private static boolean isNumericLastLabel(String host) {
    return NUMERIC_LABEL_PATTERN.matcher(host.substring(host.lastIndexOf('.') + 1)).matches();
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

  @Nullable
  private static String normalizeHost(@Nullable String host) {
    if (Strings.isNullOrEmpty(host)) {
      return null;
    }
    String normalized = host.endsWith(".") ? host.substring(0, host.length() - 1) : host;
    return normalized.isEmpty() || normalized.endsWith(".") ? null : normalized.toLowerCase(Locale.ROOT);
  }
}
