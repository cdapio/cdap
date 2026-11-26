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

import com.google.common.net.InetAddresses;

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

  private static void assertRejected(OAuthUrlValidator validator, String url, boolean requireResolvable) {
    try {
      validator.validate(url, requireResolvable);
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

    Assert.assertEquals("https://oauth.example.invalid/token",
                        validator.validate("https://oauth.example.invalid/token", true).toString());

    for (String badUrl : new String[] {
        "http://oauth.example.invalid/token", "ftp://oauth.example.invalid/token", "file:///etc/passwd",
        "https://user:pass@oauth.example.invalid/token", "https://user@oauth.example.invalid/token",
        "https://oauth.example.invalid:0/token", "https://[fe80::1%25eth0]/token", "https:///token", ""
    }) {
      assertRejected(validator, badUrl, false);
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
        ipv6Bytes("mapped-imds", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 169, 254, 169, 254),
        ipv6Bytes("mapped-rfc1918", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 10, 0, 0, 1),
        ipv6Bytes("mapped-loopback", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 127, 0, 0, 1),
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

    assertRejected(validator, "https://mixed.example.invalid/token", true);
    validator.validate("https://unknown.example.invalid/token", false);
    try {
      validator.validate("https://unknown.example.invalid/token", true);
      Assert.fail("Expected UnknownHostException for unresolvable host");
    } catch (UnknownHostException expected) {
      // expected
    }
  }

  @Test
  public void testIpLiteralsAndLocalhostAreValidatedWithoutDns() throws Exception {
    OAuthUrlValidator noDns = OAuthUrlValidator.forTesting(true, false, host -> {
      throw new AssertionError("DNS must not be used for " + host);
    });

    for (String badUrl : new String[] {
        "https://0177.0.0.1/token", "https://012.0.0.1/token", "https://2130706433/token",
        "https://127.1/token", "https://0x7f.0.0.1/token", "https://[::ffff:0177.0.0.1]/token",
        "https://169.254.169.254/token", "https://10.0.0.1/token",
        "https://[::ffff:169.254.169.254]/token", "https://[64:ff9b::a9fe:a9fe]/token",
        "https://localhost/token", "https://LOCALHOST./token", "https://api.localhost/token"
    }) {
      assertRejected(noDns, badUrl, false);
    }

    noDns.validate("https://8.8.8.8/token", true);
    noDns.validate("https://[2001:4860:4860::8888]/token", true);
  }
}
