/*
 * Copyright © 2014-2019 Cask Data, Inc.
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
package io.cdap.cdap.common.utils;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.zookeeper.coordination.DiscoverableCodec;
import org.apache.twill.discovery.Discoverable;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Utility class to provide methods for common network related operations.
 */
public final class Networks {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Discoverable.class, new DiscoverableCodec())
    .create();
  private static final Type DISCOVERABLE_SET = new TypeToken<Set<Discoverable>>() { }.getType();

  /**
   * Resolves the given hostname into {@link InetAddress}.
   *
   * @param hostname The hostname in String. If {@code null}, return localhost.
   * @param onErrorAddress InetAddress to return if the given hostname cannot be resolved.
   * @return An {@link InetAddress} of the resolved hostname.
   */
  public static InetAddress resolve(String hostname, InetAddress onErrorAddress) {
    try {
      if (hostname != null) {
        return InetAddress.getByName(hostname);
      } else {
        return InetAddress.getLocalHost();
      }
    } catch (UnknownHostException e) {
      return onErrorAddress;
    }
  }

  /**
   * Find a random free port in localhost for binding.
   * @return A port number or -1 for failure.
   */
  public static int getRandomPort() {
    try {
      try (ServerSocket socket = new ServerSocket(0)) {
        return socket.getLocalPort();
      }
    } catch (IOException e) {
      return -1;
    }
  }

  /**
   * Gets the IP address from the given {@link SocketAddress}
   *
   * @return the ip address or {@code null} if not able to ge the ip address
   */
  @Nullable
  public static String getIP(@Nullable SocketAddress address) {
    if (!(address instanceof InetSocketAddress)) {
      return null;
    }
    InetAddress inetAddress = ((InetSocketAddress) address).getAddress();
    if (inetAddress == null) {
      return null;
    }
    return inetAddress.getHostAddress();
  }

  /**
   * Adds the {@link Discoverable} into the given {@link CConfiguration} with the given key.
   * It is expected to be read back by the {@link #getDiscoverables(CConfiguration, String)} method.
   *
   * @param cConf the configuration to set to
   * @param key the configuration key
   * @param discoverable the {@link Discoverable} to set
   */
  public static void addDiscoverable(CConfiguration cConf, String key, Discoverable discoverable) {
    Set<Discoverable> currentSet = parseDiscoverables(cConf, key);
    currentSet.add(discoverable);
    cConf.set(key, GSON.toJson(currentSet, DISCOVERABLE_SET));
  }

  /**
   * Removes the given {@link Discoverable} from the given {@link CConfiguration} stored with the given key.
   *
   * @param cConf the configuration to remove from
   * @param key the configuration key
   * @param discoverable the {@link Discoverable} to remove
   */
  public static void removeDiscoverable(CConfiguration cConf, String key, Discoverable discoverable) {
    Set<Discoverable> currentSet = parseDiscoverables(cConf, key);
    // TODO: A workaround for TWILL-264. Need to wait for 0.14 release of Twill.
    currentSet.removeIf(
      d -> Objects.equals(d.getName(), discoverable.getName())
        && Objects.equals(d.getSocketAddress(), discoverable.getSocketAddress())
        && Arrays.equals(d.getPayload(), discoverable.getPayload()));

    if (currentSet.isEmpty()) {
      cConf.unset(key);
    } else {
      cConf.set(key, GSON.toJson(currentSet, DISCOVERABLE_SET));
    }
  }

  /**
   * Gets a set of {@link Discoverable}es from the given {@link CConfiguration} with the given key. It expects
   * the value is set by the {@link #addDiscoverable(CConfiguration, String, Discoverable)} method.
   * The returned addresses are not resolved.
   *
   * @param cConf the configuration to read from
   * @param key the configuration key
   * @return the a set of addresses or empty set if the key doesn't exists / has empty value
   * @throws NumberFormatException if failed to parse the port
   * @throws IllegalArgumentException if the value is not in correct format
   */
  public static Set<Discoverable> getDiscoverables(CConfiguration cConf, String key) {
    return Collections.unmodifiableSet(parseDiscoverables(cConf, key));
  }

  /**
   * Sets the {@link InetSocketAddress} into the given {@link CConfiguration} with the given key.
   * It is expected to be read back by the {@link #getAddress(CConfiguration, String)} method.
   *
   * @param cConf the configuration to set to
   * @param key the configuration key
   * @param addr the {@link InetSocketAddress} to set
   */
  public static void setAddress(CConfiguration cConf, String key, InetSocketAddress addr) {
    cConf.set(key, String.format("%s:%d", addr.getHostName(), addr.getPort()));
  }

  /**
   * Gets a {@link InetSocketAddress} from the given {@link CConfiguration} with the given key. It expects
   * the value in the format of {@code host:port}. The returned address is not resolved.
   *
   * @param cConf the configuration to read from
   * @param key the configuration key
   * @return the address or {@code null} if the key doesn't exists
   * @throws NumberFormatException if failed to parse the port
   * @throws IllegalArgumentException if the value is not in correct format
   */
  @Nullable
  public static InetSocketAddress getAddress(CConfiguration cConf, String key) {
    // Look it up from the configuration
    String value = cConf.get(key);

    // If not found, return null
    if (Strings.isNullOrEmpty(value)) {
      return null;
    }
    return parseAddress(value);
  }

  /**
   * Gets a set of {@link Discoverable} from the value of the given key in the configuration.
   */
  private static Set<Discoverable> parseDiscoverables(CConfiguration cConf, String key) {
    Set<Discoverable> discoverables = GSON.fromJson(cConf.get(key), DISCOVERABLE_SET);
    return discoverables == null ? new LinkedHashSet<>() : new LinkedHashSet<>(discoverables);
  }

  /**
   * Parses a string value of form {@code host:port} into an unresolved {@link InetSocketAddress}.
   */
  private static InetSocketAddress parseAddress(String value) {
    int idx = value.lastIndexOf(':');
    if (idx < 0) {
      throw new IllegalArgumentException("Failed to parse address from " + value
                                           + ". Expected to be in the format of host:port.");
    }

    // Parse and return the address
    return InetSocketAddress.createUnresolved(value.substring(0, idx), Integer.parseInt(value.substring(idx + 1)));
  }

  private Networks() {
  }
}
