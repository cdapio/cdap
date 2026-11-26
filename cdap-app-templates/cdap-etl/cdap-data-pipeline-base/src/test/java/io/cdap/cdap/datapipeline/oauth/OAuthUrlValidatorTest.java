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

import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
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

  private static InetAddress ip(String literal) {
    return InetAddresses.forString(literal);
  }

  private static Inet6Address ipv6Bytes(String label, int... bytes) throws UnknownHostException {
    byte[] raw = new byte[16];
    for (int i = 0; i < 16; i++) {
      raw[i] = (byte) bytes[i];
    }
    return Inet6Address.getByAddress(label, raw, null);
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

  private static void assertRejected(OAuthUrlValidator validator, String url,
                                     HostResolution resolution, AllowedHosts allowlist) {
    try {
      validator.validate(url, resolution, allowlist);
      Assert.fail("Expected rejection for URL: " + url);
    } catch (IllegalArgumentException | MalformedURLException expected) {
      // expected
    } catch (UnknownHostException e) {
      Assert.fail("Unexpected UnknownHostException for URL " + url + ": " + e);
    }
  }

  @Test
  public void testRequiresHttpsAndRejectsMalformedSyntax() throws Exception {
    Map<String, InetAddress[]> dns = new HashMap<>();
    dns.put("oauth.example.invalid", new InetAddress[] {ip("93.184.216.34")});
    OAuthUrlValidator validator = validatorWithDns(dns);

    OAuthUrlValidator.ValidatedUrl valid = validator.validate(
        "https://oauth.example.invalid/token", HostResolution.REQUIRE_RESOLVABLE, AllowedHosts.allowAll());
    Assert.assertEquals("oauth.example.invalid", valid.getNormalizedHost());
    Assert.assertEquals(443, valid.getEffectivePort());
    Assert.assertEquals(1, valid.getResolvedAddresses().size());

    for (String badUrl : new String[] {
        "http://oauth.example.invalid/token", "ftp://oauth.example.invalid/token", "file:///etc/passwd",
        "https://user:pass@oauth.example.invalid/token", "https://user@oauth.example.invalid/token",
        "https://oauth.example.invalid:0/token", "https://[fe80::1%25eth0]/token", "https:///token", ""
    }) {
      assertRejected(validator, badUrl, HostResolution.ALLOW_UNRESOLVED, AllowedHosts.allowAll());
    }
  }

  @Test
  public void testBlocksAllUnsafeIpv4AndAllowsPublicBoundaries() {
    for (String blocked : new String[] {
        "0.0.0.0", "0.0.0.1", "127.0.0.1", "127.255.255.254", "10.0.0.1", "172.16.0.1", "172.31.255.254",
        "192.168.1.1", "169.254.169.254", "100.64.0.1", "100.127.255.254", "198.18.0.1", "198.19.255.254",
        "224.0.0.1", "240.0.0.1", "255.255.255.255"
    }) {
      try {
        OAuthUrlValidator.validateInetAddress(ip(blocked), false);
        Assert.fail("Expected IPv4 address to be blocked: " + blocked);
      } catch (IllegalArgumentException expected) {
        // expected
      }
    }
    for (String allowed : new String[] {
        "8.8.8.8", "93.184.216.34", "100.63.255.255", "100.128.0.1",
        "198.17.255.255", "198.20.0.1", "172.15.255.255", "172.32.0.1"
    }) {
      OAuthUrlValidator.validateInetAddress(ip(allowed), false);
    }
  }

  @Test
  public void testBlocksUnsafeIpv6AndEmbeddedIpv4Representations() throws Exception {
    InetAddress[] blocked = {
        ip("::"), ip("::1"), ip("fe80::1"), ip("fc00::1"), ip("fd12:3456::1"), ip("fec0::1"), ip("ff02::1"),
        ip("64:ff9b:1::1"), ip("100::1"),
        // Real 16-byte Inet6Address IPv4-mapped ::ffff:169.254.169.254, ::ffff:10.0.0.1, ::ffff:127.0.0.1
        ipv6Bytes("mapped-imds", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 169, 254, 169, 254),
        ipv6Bytes("mapped-rfc1918", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 10, 0, 0, 1),
        ipv6Bytes("mapped-loopback", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 127, 0, 0, 1),
        // SIIT, well-known NAT64, IPv4-compatible, 6to4, Teredo (client & server), ISATAP (0000 & 0200)
        ip("::ffff:0:169.254.169.254"), ip("64:ff9b::169.254.169.254"), ip("64:ff9b::10.0.0.1"),
        ip("::169.254.169.254"), ip("2002:a9fe:a9fe::1"), ip("2002:0a00:0001::1"),
        ipv6Bytes("teredo-client", 0x20, 0x01, 0, 0, 8, 8, 8, 8, 0, 0, 0, 0,
                  ~169 & 0xff, ~254 & 0xff, ~169 & 0xff, ~254 & 0xff),
        ipv6Bytes("teredo-server", 0x20, 0x01, 0, 0, 169, 254, 169, 254, 0, 0, 0, 0,
                  ~8 & 0xff, ~8 & 0xff, ~8 & 0xff, ~8 & 0xff),
        ip("2001:4860::5efe:169.254.169.254"), ip("2001:4860::200:5efe:10.0.0.1")
    };
    for (InetAddress addr : blocked) {
      try {
        OAuthUrlValidator.validateInetAddress(addr, false);
        Assert.fail("Expected IPv6 address to be blocked: " + addr.getHostAddress());
      } catch (IllegalArgumentException expected) {
        // expected
      }
    }

    OAuthUrlValidator.validateInetAddress(ip("2001:4860:4860::8888"), false);
    OAuthUrlValidator.validateInetAddress(
        ipv6Bytes("mapped-public", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 8, 8, 8, 8), false);
    OAuthUrlValidator.validateInetAddress(ip("64:ff9b::8.8.8.8"), false);
    OAuthUrlValidator.validateInetAddress(ip("::1"), true);
  }

  @Test
  public void testMultiRecordDnsFailsClosedAndHandlesResolutionModes() throws Exception {
    Map<String, InetAddress[]> dns = new HashMap<>();
    dns.put("mixed.example.invalid", new InetAddress[] {ip("8.8.8.8"), ip("169.254.169.254")});
    OAuthUrlValidator validator = validatorWithDns(dns);

    assertRejected(validator, "https://mixed.example.invalid/token",
                   HostResolution.REQUIRE_RESOLVABLE, AllowedHosts.allowAll());

    // ALLOW_UNRESOLVED succeeds with empty list; REQUIRE_RESOLVABLE throws retryable UnknownHostException
    Assert.assertTrue(validator.validate("https://unknown.example.invalid/token",
                                         HostResolution.ALLOW_UNRESOLVED, AllowedHosts.allowAll())
                          .getResolvedAddresses().isEmpty());
    try {
      validator.validate("https://unknown.example.invalid/token",
                         HostResolution.REQUIRE_RESOLVABLE, AllowedHosts.allowAll());
      Assert.fail("Expected UnknownHostException for unresolvable host");
    } catch (UnknownHostException expected) {
      // expected
    }

    OAuthUrlValidator emptyDns = OAuthUrlValidator.forTesting(true, false, host -> new InetAddress[0]);
    Assert.assertTrue(emptyDns.validate("https://empty.example.invalid/token",
                                        HostResolution.ALLOW_UNRESOLVED, AllowedHosts.allowAll())
                          .getResolvedAddresses().isEmpty());
    try {
      emptyDns.validate("https://empty.example.invalid/token",
                        HostResolution.REQUIRE_RESOLVABLE, AllowedHosts.allowAll());
      Assert.fail("Expected UnknownHostException for empty DNS answer");
    } catch (UnknownHostException expected) {
      Assert.assertTrue(expected.getMessage().contains("Unresolvable"));
    }
  }

  @Test
  public void testIpLiteralsAndLocalhostAreValidatedWithoutDns() throws Exception {
    OAuthUrlValidator noDns = OAuthUrlValidator.forTesting(true, false, host -> {
      throw new AssertionError("DNS must not be used for " + host);
    });

    for (String badUrl : new String[] {
        // Ambiguous octal/hex/short/integer IPv4 forms
        "https://0177.0.0.1/token", "https://012.0.0.1/token", "https://0251.0376.0251.0376/token",
        "https://2130706433/token", "https://127.1/token", "https://0x7f.0.0.1/token",
        "https://1.2.3.0x4/token", "https://1.2.3.256/token", "https://[::ffff:0177.0.0.1]/token",
        // Disallowed IP literals and loopback hostnames
        "https://169.254.169.254/token", "https://10.0.0.1/token",
        "https://[::ffff:169.254.169.254]/token", "https://[64:ff9b::a9fe:a9fe]/token",
        "https://localhost/token", "https://LOCALHOST./token", "https://api.localhost/token"
    }) {
      assertRejected(noDns, badUrl, HostResolution.ALLOW_UNRESOLVED, AllowedHosts.allowAll());
    }

    // Canonical public literals are accepted and pinned without DNS
    OAuthUrlValidator.ValidatedUrl v4 = noDns.validate("https://8.8.8.8/token",
                                                       HostResolution.REQUIRE_RESOLVABLE, AllowedHosts.allowAll());
    Assert.assertEquals("8.8.8.8", v4.getNormalizedHost());
    Assert.assertEquals(ImmutableList.of(ip("8.8.8.8")), v4.getResolvedAddresses());

    OAuthUrlValidator.ValidatedUrl v6 = noDns.validate("https://[2001:4860:4860::8888]/token",
                                                       HostResolution.REQUIRE_RESOLVABLE, AllowedHosts.allowAll());
    Assert.assertEquals("2001:4860:4860:0:0:0:0:8888", v6.getNormalizedHost());
    Assert.assertEquals(1, v6.getResolvedAddresses().size());

    // A hostname with "localhost" only as a leading label is a normal DNS name
    OAuthUrlValidator unresolvable = OAuthUrlValidator.forTesting(true, false, host -> {
      throw new UnknownHostException(host);
    });
    Assert.assertTrue(unresolvable.validate("https://localhost.example.invalid/token",
                                            HostResolution.ALLOW_UNRESOLVED, AllowedHosts.allowAll())
                          .getResolvedAddresses().isEmpty());
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

    for (String bad : new String[] {
        "*", "*.", "*.com", "*.*.salesforce.com", "foo*.salesforce.com", "a.*.salesforce.com",
        "https://oauth2.googleapis.com", "oauth2.googleapis.com/token", "oauth2.googleapis.com:443",
        "user@oauth2.googleapis.com", "oauth2.googleapis.com..", ",,,",
        "93.184.216.34:443", "dead.beef:443", "::::", "2001:db8::1",
        "[2001:4860:4860::8888]", "[2001:db8::1", "[]", "8.8.8.8", "0177.0.0.1", "127.1", "2130706433", "*.0.0.1"
    }) {
      try {
        AllowedHosts.parse(bad);
        Assert.fail("Expected IllegalArgumentException for invalid allowlist pattern: " + bad);
      } catch (IllegalArgumentException expected) {
        // expected
      }
    }

    Map<String, InetAddress[]> dns = new HashMap<>();
    dns.put("oauth2.googleapis.com", new InetAddress[] {ip("8.8.8.8")});
    dns.put("internal.salesforce.com", new InetAddress[] {ip("10.0.0.5")});
    dns.put("other.example.invalid", new InetAddress[] {ip("93.184.216.34")});
    OAuthUrlValidator validator = validatorWithDns(dns);

    validator.validate("https://OAuth2.GoogleAPIs.com./token", HostResolution.REQUIRE_RESOLVABLE, allowlist);
    assertRejected(validator, "https://other.example.invalid/token", HostResolution.REQUIRE_RESOLVABLE, allowlist);
    assertRejected(validator, "https://internal.salesforce.com/token", HostResolution.REQUIRE_RESOLVABLE, allowlist);
    assertRejected(validator, "https://8.8.8.8/token", HostResolution.REQUIRE_RESOLVABLE, allowlist);
  }

  @Test
  public void testHasSameOrigin() {
    Assert.assertTrue(OAuthUrlValidator.hasSameOrigin(
        "https://login.salesforce.com./services/oauth2/token",
        "https://LOGIN.SALESFORCE.COM:443/id/00D/005"));
    Assert.assertFalse(OAuthUrlValidator.hasSameOrigin(
        "https://login.salesforce.com/token", "http://login.salesforce.com/token"));
    Assert.assertFalse(OAuthUrlValidator.hasSameOrigin(
        "https://login.salesforce.com/token", "https://login.salesforce.com:8443/token"));
    Assert.assertFalse(OAuthUrlValidator.hasSameOrigin(
        "https://user:pass@login.salesforce.com/token", "https://login.salesforce.com/token"));
    Assert.assertFalse(OAuthUrlValidator.hasSameOrigin(null, "https://login.salesforce.com/token"));
  }
}
