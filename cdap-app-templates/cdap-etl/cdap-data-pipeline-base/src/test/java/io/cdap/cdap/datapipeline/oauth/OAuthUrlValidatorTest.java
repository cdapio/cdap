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

import io.cdap.cdap.datapipeline.oauth.OAuthUrlValidator.AllowedHosts;
import io.cdap.cdap.datapipeline.oauth.OAuthUrlValidator.HostResolution;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link OAuthUrlValidator}.
 */
public class OAuthUrlValidatorTest {

  private static InetAddress ipv4(int a, int b, int c, int d) throws UnknownHostException {
    return InetAddress.getByAddress(new byte[] {(byte) a, (byte) b, (byte) c, (byte) d});
  }

  private static Inet6Address ipv6(String host, int... bytes) throws UnknownHostException {
    byte[] raw = new byte[16];
    for (int i = 0; i < 16; i++) {
      raw[i] = (byte) bytes[i];
    }
    return Inet6Address.getByAddress(host, raw, null);
  }

  private static OAuthUrlValidator validatorWithDns(Map<String, InetAddress[]> dns) {
    return OAuthUrlValidator.forTesting(true, false, host -> {
      InetAddress[] result = dns.get(host);
      if (result != null) {
        return result;
      }
      throw new UnknownHostException(host);
    });
  }

  @Test
  public void testRequiresHttpsAndRejectsMalformedSyntax() throws Exception {
    Map<String, InetAddress[]> dns = new HashMap<>();
    dns.put("oauth.example.invalid", new InetAddress[] {ipv4(93, 184, 216, 34)});
    OAuthUrlValidator validator = validatorWithDns(dns);

    OAuthUrlValidator.ValidatedUrl valid = validator.validate(
        "https://oauth.example.invalid/token", HostResolution.REQUIRE_RESOLVABLE, AllowedHosts.allowAll());
    Assert.assertEquals("oauth.example.invalid", valid.getNormalizedHost());
    Assert.assertEquals(443, valid.getEffectivePort());
    Assert.assertEquals(1, valid.getResolvedAddresses().size());

    String[] disallowedUrls = {
        "http://oauth.example.invalid/token",
        "ftp://oauth.example.invalid/token",
        "file:///etc/passwd",
        "https://user:pass@oauth.example.invalid/token",
        "https://user@oauth.example.invalid/token",
        "https://oauth.example.invalid:0/token",
        "https://[fe80::1%25eth0]/token",
        "https:///token",
        ""
    };
    for (String badUrl : disallowedUrls) {
      try {
        validator.validate(badUrl, HostResolution.ALLOW_UNRESOLVED, AllowedHosts.allowAll());
        Assert.fail("Expected rejection for URL: " + badUrl);
      } catch (IllegalArgumentException | MalformedURLException expected) {
        // expected
      }
    }
  }

  @Test
  public void testBlocksAllUnsafeIpv4AndAllowsPublicBoundaries() throws Exception {
    InetAddress[] blocked = {
        ipv4(0, 0, 0, 0),
        ipv4(0, 0, 0, 1),
        ipv4(127, 0, 0, 1),
        ipv4(127, 255, 255, 254),
        ipv4(10, 0, 0, 1),
        ipv4(172, 16, 0, 1),
        ipv4(172, 31, 255, 254),
        ipv4(192, 168, 1, 1),
        ipv4(169, 254, 169, 254),
        ipv4(100, 64, 0, 1),
        ipv4(100, 127, 255, 254),
        ipv4(198, 18, 0, 1),
        ipv4(198, 19, 255, 254),
        ipv4(224, 0, 0, 1),
        ipv4(240, 0, 0, 1),
        ipv4(255, 255, 255, 255)
    };
    for (InetAddress addr : blocked) {
      try {
        OAuthUrlValidator.validateInetAddress(addr, false);
        Assert.fail("Expected IPv4 address to be blocked: " + addr.getHostAddress());
      } catch (IllegalArgumentException expected) {
        // expected
      }
    }

    InetAddress[] allowed = {
        ipv4(8, 8, 8, 8),
        ipv4(93, 184, 216, 34),
        ipv4(100, 63, 255, 255),
        ipv4(100, 128, 0, 1),
        ipv4(198, 17, 255, 255),
        ipv4(198, 20, 0, 1),
        ipv4(172, 15, 255, 255),
        ipv4(172, 32, 0, 1)
    };
    for (InetAddress addr : allowed) {
      OAuthUrlValidator.validateInetAddress(addr, false);
    }
  }

  @Test
  public void testBlocksUnsafeIpv6AndEmbeddedIpv4Representations() throws Exception {
    Inet6Address[] blocked = {
        // :: and ::1
        ipv6("unspec", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        ipv6("loopback", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1),
        // fe80::1 (link-local), fc00::1 / fd12::1 (ULA), fec0::1 (site-local), ff02::1 (multicast)
        ipv6("linklocal", 0xfe, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1),
        ipv6("ula1", 0xfc, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1),
        ipv6("ula2", 0xfd, 0x12, 0x34, 0x56, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1),
        ipv6("sitelocal", 0xfe, 0xc0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1),
        ipv6("multicast", 0xff, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1),
        // 64:ff9b:1::1 (local-use NAT64), 100::1 (discard-only)
        ipv6("localnat64", 0, 0x64, 0xff, 0x9b, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1),
        ipv6("discard", 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1),
        // Real 16-byte Inet6Address IPv4-mapped ::ffff:169.254.169.254, ::ffff:10.0.0.1, ::ffff:127.0.0.1
        ipv6("mapped-imds", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 169, 254, 169, 254),
        ipv6("mapped-rfc1918", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 10, 0, 0, 1),
        ipv6("mapped-loopback", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 127, 0, 0, 1),
        // SIIT IPv4-translated ::ffff:0:169.254.169.254
        ipv6("siit-imds", 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0, 0, 169, 254, 169, 254),
        // Well-known NAT64 64:ff9b::169.254.169.254 and 64:ff9b::10.0.0.1
        ipv6("nat64-imds", 0, 0x64, 0xff, 0x9b, 0, 0, 0, 0, 0, 0, 0, 0, 169, 254, 169, 254),
        ipv6("nat64-rfc1918", 0, 0x64, 0xff, 0x9b, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 1),
        // IPv4-compatible ::169.254.169.254
        ipv6("compat-imds", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 169, 254, 169, 254),
        // 6to4 2002:a9fe:a9fe::1 (169.254.169.254) and 2002:0a00:0001::1 (10.0.0.1)
        ipv6("6to4-imds", 0x20, 0x02, 169, 254, 169, 254, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1),
        ipv6("6to4-rfc1918", 0x20, 0x02, 10, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1),
        // Teredo client = 169.254.169.254 (inverted: 0x56, 0x01, 0x56, 0x01) with public server 8.8.8.8
        ipv6("teredo-client", 0x20, 0x01, 0, 0, 8, 8, 8, 8, 0, 0, 0, 0, ~169 & 0xff, ~254 & 0xff,
             ~169 & 0xff, ~254 & 0xff),
        // Teredo server = 169.254.169.254 with public inverted client 8.8.8.8
        ipv6("teredo-server", 0x20, 0x01, 0, 0, 169, 254, 169, 254, 0, 0, 0, 0, ~8 & 0xff, ~8 & 0xff,
             ~8 & 0xff, ~8 & 0xff),
        // ISATAP (0000:5efe: and 0200:5efe:) with 169.254.169.254 and 10.0.0.1
        ipv6("isatap-imds", 0x20, 0x01, 0x48, 0x60, 0, 0, 0, 0, 0, 0, 0x5e, 0xfe, 169, 254, 169, 254),
        ipv6("isatap-rfc1918", 0x20, 0x01, 0x48, 0x60, 0, 0, 0, 0, 0x02, 0, 0x5e, 0xfe, 10, 0, 0, 1)
    };
    for (Inet6Address addr : blocked) {
      try {
        OAuthUrlValidator.validateInetAddress(addr, false);
        Assert.fail("Expected IPv6 address to be blocked: " + addr.getHostName() + " / " + addr.getHostAddress());
      } catch (IllegalArgumentException expected) {
        // expected
      }
    }

    // Public IPv6 and public embedded IPv4 addresses are allowed
    OAuthUrlValidator.validateInetAddress(
        ipv6("google-dns", 0x20, 0x01, 0x48, 0x60, 0x48, 0x60, 0, 0, 0, 0, 0, 0, 0, 0, 0x88, 0x88), false);
    OAuthUrlValidator.validateInetAddress(
        ipv6("mapped-public", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 8, 8, 8, 8), false);
    OAuthUrlValidator.validateInetAddress(
        ipv6("nat64-public", 0, 0x64, 0xff, 0x9b, 0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8), false);
    // Test-mode allowLoopback allows ::1 while still blocking non-loopback private/link-local addresses
    OAuthUrlValidator.validateInetAddress(
        ipv6("loopback-allowed", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1), true);
  }

  @Test
  public void testMultiRecordDnsFailsClosedAndHandlesResolutionModes() throws Exception {
    Map<String, InetAddress[]> dns = new HashMap<>();
    dns.put("mixed.example.invalid", new InetAddress[] {ipv4(8, 8, 8, 8), ipv4(169, 254, 169, 254)});
    OAuthUrlValidator validator = validatorWithDns(dns);

    try {
      validator.validate("https://mixed.example.invalid/token",
                         HostResolution.REQUIRE_RESOLVABLE, AllowedHosts.allowAll());
      Assert.fail("Expected rejection when any resolved record is disallowed");
    } catch (IllegalArgumentException expected) {
      Assert.assertTrue(expected.getMessage().contains("169.254.169.254"));
    }

    // Unresolvable host succeeds with ALLOW_UNRESOLVED (empty resolved list) and fails with REQUIRE_RESOLVABLE
    OAuthUrlValidator.ValidatedUrl unresolved = validator.validate(
        "https://unknown.example.invalid/token", HostResolution.ALLOW_UNRESOLVED, AllowedHosts.allowAll());
    Assert.assertTrue(unresolved.getResolvedAddresses().isEmpty());

    try {
      validator.validate("https://unknown.example.invalid/token",
                         HostResolution.REQUIRE_RESOLVABLE, AllowedHosts.allowAll());
      Assert.fail("Expected rejection for unresolvable host under REQUIRE_RESOLVABLE");
    } catch (IllegalArgumentException expected) {
      Assert.assertTrue(expected.getMessage().contains("Unresolvable"));
    }
  }

  @Test
  public void testAllowedHostsParsingMatchingAndCannotBypassIpBlocks() throws Exception {
    Assert.assertFalse(AllowedHosts.parse(null).isRestricted());
    Assert.assertFalse(AllowedHosts.parse("   ").isRestricted());

    AllowedHosts allowlist = AllowedHosts.parse(" oauth2.googleapis.com , *.salesforce.com., Login.Okta.com ");
    Assert.assertTrue(allowlist.isRestricted());
    Assert.assertTrue(allowlist.isAllowed("oauth2.googleapis.com"));
    Assert.assertTrue(allowlist.isAllowed("acme.salesforce.com"));
    Assert.assertTrue(allowlist.isAllowed("sub.acme.salesforce.com"));
    Assert.assertTrue(allowlist.isAllowed("login.okta.com"));
    Assert.assertFalse(allowlist.isAllowed("salesforce.com"));
    Assert.assertFalse(allowlist.isAllowed("evilsalesforce.com"));
    Assert.assertFalse(allowlist.isAllowed("attacker.example.com"));

    String[] invalidPatterns = {
        "*", "*.", "*.com", "*.*.salesforce.com", "foo*.salesforce.com", "a.*.salesforce.com",
        "https://oauth2.googleapis.com", "oauth2.googleapis.com/token", "oauth2.googleapis.com:443",
        "user@oauth2.googleapis.com", "oauth2.googleapis.com..", ",,,"
    };
    for (String bad : invalidPatterns) {
      try {
        AllowedHosts.parse(bad);
        Assert.fail("Expected IllegalArgumentException for invalid allowlist pattern: " + bad);
      } catch (IllegalArgumentException expected) {
        // expected
      }
    }

    // Even when a host is explicitly in AllowedHosts, private/IMDS IPs are still blocked
    Map<String, InetAddress[]> dns = new HashMap<>();
    dns.put("oauth2.googleapis.com", new InetAddress[] {ipv4(8, 8, 8, 8)});
    dns.put("internal.salesforce.com", new InetAddress[] {ipv4(10, 0, 0, 5)});
    dns.put("other.example.invalid", new InetAddress[] {ipv4(93, 184, 216, 34)});
    OAuthUrlValidator validator = validatorWithDns(dns);

    validator.validate("https://OAuth2.GoogleAPIs.com./token", HostResolution.REQUIRE_RESOLVABLE, allowlist);
    try {
      validator.validate("https://other.example.invalid/token", HostResolution.REQUIRE_RESOLVABLE, allowlist);
      Assert.fail("Expected non-allowlisted host to be rejected");
    } catch (IllegalArgumentException expected) {
      Assert.assertTrue(expected.getMessage().contains("allowlist"));
    }
    try {
      validator.validate("https://internal.salesforce.com/token", HostResolution.REQUIRE_RESOLVABLE, allowlist);
      Assert.fail("Expected allowlisted host resolving to RFC 1918 IP to still be blocked");
    } catch (IllegalArgumentException expected) {
      Assert.assertTrue(expected.getMessage().contains("10.0.0.5"));
    }
  }

  @Test
  public void testHasSameOrigin() {
    Assert.assertTrue(OAuthUrlValidator.hasSameOrigin(
        "https://login.salesforce.com./services/oauth2/token",
        "https://LOGIN.SALESFORCE.COM:443/id/00D/005"));
    Assert.assertFalse(OAuthUrlValidator.hasSameOrigin(
        "https://login.salesforce.com/token",
        "http://login.salesforce.com/token"));
    Assert.assertFalse(OAuthUrlValidator.hasSameOrigin(
        "https://login.salesforce.com/token",
        "https://login.salesforce.com:8443/token"));
    Assert.assertFalse(OAuthUrlValidator.hasSameOrigin(
        "https://user:pass@login.salesforce.com/token",
        "https://login.salesforce.com/token"));
    Assert.assertFalse(OAuthUrlValidator.hasSameOrigin(null, "https://login.salesforce.com/token"));
  }
}
