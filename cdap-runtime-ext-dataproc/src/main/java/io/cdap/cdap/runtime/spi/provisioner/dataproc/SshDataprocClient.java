/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.provisioner.dataproc;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.compute.model.AccessConfig;
import com.google.api.services.compute.model.Firewall;
import com.google.api.services.compute.model.FirewallList;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.Network;
import com.google.cloud.dataproc.v1.ClusterControllerClient;
import com.google.cloud.dataproc.v1.GceClusterConfig;
import io.cdap.cdap.runtime.spi.common.DataprocUtils;
import io.cdap.cdap.runtime.spi.common.IPRange;
import io.cdap.cdap.runtime.spi.provisioner.Node;
import io.cdap.cdap.runtime.spi.provisioner.RetryableProvisionException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper around the dataproc client that adheres to our configuration settings. Creates Dataproc
 * clusters that are accessible through SSH.
 */
class SshDataprocClient extends DataprocClient {

  private static final Logger LOG = LoggerFactory.getLogger(SshDataprocClient.class);
  private static final List<IPRange> PRIVATE_IP_RANGES = DataprocUtils.parseIpRanges(
      Arrays.asList("10.0.0.0/8",
          "172.16.0.0/12",
          "192.168.0.0/16"));

  SshDataprocClient(DataprocConf conf, ClusterControllerClient client,
      ComputeFactory computeFactory) {
    super(conf, client, computeFactory);
  }

  @Override
  protected void addNetworkTags(GceClusterConfig.Builder clusterConfig, Network networkInfo,
      boolean internalIpOnly) throws RetryableProvisionException, IOException {

    // if public key is not null that means ssh is used to launch / monitor job on dataproc
    int maxTags = Math.max(0, DataprocConf.MAX_NETWORK_TAGS - clusterConfig.getTagsCount());
    List<String> tags = getFirewallTargetTags(networkInfo, internalIpOnly);
    if (tags.size() > maxTags) {
      LOG.warn("No more than 64 tags can be added. Firewall tags ignored: {}",
          tags.subList(maxTags, tags.size()));
    }
    tags.stream().limit(maxTags).forEach(clusterConfig::addTags);
  }

  @Override
  protected Node getNode(Node.Type type, String zone, String nodeName) throws IOException {
    Instance instance;
    try {
      instance = getOrCreateCompute().instances().get(conf.getProjectId(), zone, nodeName)
          .execute();
    } catch (GoogleJsonResponseException e) {
      // this can happen right after a cluster is created
      if (e.getStatusCode() == 404) {
        return new Node(nodeName, Node.Type.UNKNOWN, "", -1L, Collections.emptyMap());
      }
      throw e;
    }
    Map<String, String> properties = new HashMap<>();

    // Dataproc cluster node should only have exactly one network
    instance.getNetworkInterfaces().stream().findFirst().ifPresent(networkInterface -> {
      // if the cluster does not have an external ip then then access config is null
      if (networkInterface.getAccessConfigs() != null) {
        for (AccessConfig accessConfig : networkInterface.getAccessConfigs()) {
          if (accessConfig.getNatIP() != null) {
            properties.put("ip.external", accessConfig.getNatIP());
            break;
          }
        }
      }
      properties.put("ip.internal", networkInterface.getNetworkIP());
    });

    long ts;
    try {
      // something like 2018-04-16T12:09:03.943-07:00
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSSX");
      ts = sdf.parse(instance.getCreationTimestamp()).getTime();
    } catch (ParseException | NumberFormatException e) {
      LOG.debug("Fail to parse creation ts {}", instance.getCreationTimestamp(), e);
      ts = -1L;
    }

    // For internal IP only cluster, nodes only have ip.internal.
    String ip = properties.get("ip.external");
    if (ip == null) {
      ip = properties.get("ip.internal");
    }
    return new Node(nodeName, type, ip, ts, properties);
  }

  /**
   * Finds ingress firewall rules for the configured network that matches the required firewall port
   * as defined in {@link FirewallPort}.
   *
   * @return a {@link Collection} of tags that need to be added to the VM to have those firewall
   *     rules applies
   * @throws IOException If failed to discover those firewall rules
   */
  private List<String> getFirewallTargetTags(Network network, boolean useInternalIp)
      throws IOException, RetryableProvisionException {
    FirewallList firewalls;
    try {
      firewalls = getOrCreateCompute().firewalls().list(conf.getNetworkHostProjectId()).execute();
    } catch (Exception e) {
      handleRetryableExceptions(e);
      throw new DataprocRuntimeException(e);
    }

    List<String> tags = new ArrayList<>();
    Set<FirewallPort> requiredPorts = EnumSet.allOf(FirewallPort.class);
    // Iterate all firewall rules and see if it has ingress rules for all required firewall port.
    for (Firewall firewall : Optional.ofNullable(firewalls.getItems())
        .orElse(Collections.emptyList())) {
      // network is a url like https://www.googleapis.com/compute/v1/projects/<project>/<region>/networks/<name>
      // we want to get the last section of the path and compare to the configured network name
      int idx = firewall.getNetwork().lastIndexOf('/');
      String networkName =
          idx >= 0 ? firewall.getNetwork().substring(idx + 1) : firewall.getNetwork();
      if (!networkName.equals(network.getName())) {
        continue;
      }

      String direction = firewall.getDirection();
      if (!"INGRESS".equals(direction) || firewall.getAllowed() == null) {
        continue;
      }

      if (useInternalIp) {
        // If the Dataproc cluster is using internal IP only, we are only interested in firewall rule that has source
        // IP range overlap with one of the private IP block or doesn't have source IP at all.
        // This is because if Dataproc cluster is using internal IP, the CDAP itself must be running inside one of the
        // private IP blocks in order to be able to communicate with Dataproc.
        try {
          List<IPRange> sourceRanges = Optional.ofNullable(firewall.getSourceRanges())
              .map(DataprocUtils::parseIpRanges)
              .orElse(Collections.emptyList());

          if (!sourceRanges.isEmpty()) {
            boolean isPrivate = PRIVATE_IP_RANGES.stream()
                .anyMatch(privateRange -> sourceRanges.stream().anyMatch(privateRange::isOverlap));
            if (!isPrivate) {
              continue;
            }
          }
        } catch (Exception e) {
          LOG.warn("Failed to parse source ranges from firewall rule {}", firewall.getName(), e);
        }
      }

      for (Firewall.Allowed allowed : firewall.getAllowed()) {
        String protocol = allowed.getIPProtocol();
        boolean addTag = false;
        if ("all".equalsIgnoreCase(protocol)) {
          requiredPorts.clear();
          addTag = true;
        } else if ("tcp".equalsIgnoreCase(protocol) && isPortAllowed(allowed.getPorts(),
            FirewallPort.SSH.port)) {
          requiredPorts.remove(FirewallPort.SSH);
          addTag = true;
        }
        if (addTag && firewall.getTargetTags() != null && !firewall.getTargetTags().isEmpty()) {
          tags.add(firewall.getTargetTags().iterator().next());
        }
      }
    }

    if (!requiredPorts.isEmpty()) {
      String portList = requiredPorts.stream().map(p -> String.valueOf(p.port))
          .collect(Collectors.joining(","));
      throw new IllegalArgumentException(String.format(
          "Could not find an ingress firewall rule for network '%s' in project '%s' for ports '%s'. "

              + "Please create a rule to allow incoming traffic on those ports for your IP range.",
          network.getName(), conf.getNetworkHostProjectId(), portList));
    }
    return tags;
  }

  /**
   * Returns if the given port is allowed by the list of allowed ports. The allowed ports is in
   * format as allowed by GCP firewall rule.
   */
  private boolean isPortAllowed(@Nullable List<String> allowedPorts, int port) {
    if (allowedPorts == null) {
      return true;
    }
    for (String allowedPort : allowedPorts) {
      int idx = allowedPort.lastIndexOf('-');
      try {
        // This is a port range specification in format of "startPort-endPort" (e.g. 0-65535)
        if (idx > 0) {
          int fromPort = Integer.parseInt(allowedPort.substring(0, idx));
          int toPort = Integer.parseInt(allowedPort.substring(idx + 1));
          if (port >= fromPort && port <= toPort) {
            return true;
          }
        } else if (port == Integer.parseInt(allowedPort)) {
          return true;
        }
      } catch (NumberFormatException e) {
        LOG.warn("Ignoring firewall allowed port value '{}' due to parse error.", allowedPort, e);
      }
    }
    return false;
  }

  /**
   * Firewall ports that we're concerned about.
   */
  private enum FirewallPort {
    SSH(22);

    private final int port;

    FirewallPort(int port) {
      this.port = port;
    }
  }
}
