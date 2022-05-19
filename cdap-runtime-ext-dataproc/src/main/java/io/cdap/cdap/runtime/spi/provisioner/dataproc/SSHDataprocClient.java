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
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.AccessConfig;
import com.google.api.services.compute.model.Firewall;
import com.google.api.services.compute.model.FirewallList;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.Network;
import com.google.api.services.compute.model.NetworkPeering;
import com.google.cloud.dataproc.v1.ClusterControllerClient;
import com.google.cloud.dataproc.v1.GceClusterConfig;
import io.cdap.cdap.runtime.spi.common.DataprocUtils;
import io.cdap.cdap.runtime.spi.common.IPRange;
import io.cdap.cdap.runtime.spi.provisioner.Node;
import io.cdap.cdap.runtime.spi.provisioner.RetryableProvisionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * Wrapper around the dataproc client that adheres to our configuration settings. Creates Dataproc clusters that
 * are accessible through SSH.
 */
class SSHDataprocClient extends DataprocClient {

  private static final Logger LOG = LoggerFactory.getLogger(SSHDataprocClient.class);
  private static final List<IPRange> PRIVATE_IP_RANGES = DataprocUtils.parseIPRanges(Arrays.asList("10.0.0.0/8",
                                                                                                   "172.16.0.0/12",
                                                                                                   "192.168.0.0/16"));

  SSHDataprocClient(DataprocConf conf, ClusterControllerClient client, Compute compute) {
    super(conf, client, compute);
  }

  @Override
  protected void setNetworkConfigs(GceClusterConfig.Builder clusterConfig, String network,
                                   boolean privateInstance) throws RetryableProvisionException, IOException {
    String networkHostProjectId = conf.getNetworkHostProjectID();
    String subnet = conf.getSubnet();
    Network networkInfo = getNetworkInfo(networkHostProjectId, network, compute);

    List<String> subnets = networkInfo.getSubnetworks();
    if (subnet != null && !subnetExists(subnets, subnet)) {
      throw new IllegalArgumentException(String.format("Subnet '%s' does not exist in network '%s' in project '%s'. "
                                                         + "Please use a different subnet.",
                                                       subnet, network, networkHostProjectId));
    }

    // if the network uses custom subnets, a subnet must be provided to the dataproc api
    boolean autoCreateSubnet = networkInfo.getAutoCreateSubnetworks() != null && networkInfo.getAutoCreateSubnetworks();
    if (!autoCreateSubnet) {
      // if the network uses custom subnets but none exist, error out
      if (subnets == null || subnets.isEmpty()) {
        throw new IllegalArgumentException(String.format("Network '%s' in project '%s' does not contain any subnets. "
                                                           + "Please create a subnet or use a different network.",
                                                         network, networkHostProjectId));
      }
    }

    subnet = chooseSubnet(network, subnets, subnet, conf.getRegion());

    // subnets are unique within a location, not within a network, which is why these configs are mutually exclusive.
    clusterConfig.setSubnetworkUri(subnet);

    //Add any defined Network Tags
    clusterConfig.addAllTags(conf.getNetworkTags());
    boolean internalIPOnly = isInternalIPOnly(networkInfo, privateInstance);

    // if public key is not null that means ssh is used to launch / monitor job on dataproc
    int maxTags = Math.max(0, DataprocConf.MAX_NETWORK_TAGS - clusterConfig.getTagsCount());
    List<String> tags = getFirewallTargetTags(networkInfo, internalIPOnly);
    if (tags.size() > maxTags) {
      LOG.warn("No more than 64 tags can be added. Firewall tags ignored: {}", tags.subList(maxTags, tags.size()));
    }
    tags.stream().limit(maxTags).forEach(clusterConfig::addTags);

    // if internal ip is preferred then create dataproc cluster without external ip for better security
    clusterConfig.setInternalIpOnly(internalIPOnly);
  }

  @Override
  protected Node getNode(Node.Type type, String zone, String nodeName) throws IOException {
    Instance instance;
    try {
      instance = compute.instances().get(conf.getProjectId(), zone, nodeName).execute();
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

  private static PeeringState getPeeringState(String systemProjectId, String systemNetwork, Network networkInfo) {
    // note: vpc network is a global resource.
    // https://cloud.google.com/compute/docs/regions-zones/global-regional-zonal-resources#globalresources
    String systemNetworkPath = String.format("https://www.googleapis.com/compute/v1/projects/%s/global/networks/%s",
                                             systemProjectId, systemNetwork);

    LOG.trace(String.format("Self link for the system network is %s", systemNetworkPath));
    List<NetworkPeering> peerings = networkInfo.getPeerings();
    // if the customer does not has a peering established at all the peering list is null
    if (peerings == null) {
      return PeeringState.NONE;
    }
    for (NetworkPeering peering : peerings) {
      if (!systemNetworkPath.equals(peering.getNetwork())) {
        continue;
      }
      return peering.getState().equals("ACTIVE") ? PeeringState.ACTIVE : PeeringState.INACTIVE;
    }
    return PeeringState.NONE;
  }

  private static boolean subnetExists(List<String> subnets, String subnet) {
    // subnets are of the form
    // "https://www.googleapis.com/compute/v1/projects/<project>/regions/<region>/subnetworks/<name>"
    // the provided subnet can be the full URI but is most often just the name
    for (String networkSubnet : subnets) {
      if (networkSubnet.equals(subnet) || networkSubnet.endsWith("subnetworks/" + subnet)) {
        return true;
      }
    }
    return false;
  }

  // subnets are identified as
  // "https://www.googleapis.com/compute/v1/projects/<project>/regions/<region>/subnetworks/<name>"
  // a subnet in the same region as the dataproc cluster must be chosen. If a subnet name is provided then the subnet
  // will be choosen and the region will be picked on basis of the given zone. If a subnet name is not provided then
  // any subnetwork in the region of the given zone will be picked.
  private static String chooseSubnet(String network, List<String> subnets, @Nullable String subnet, String region) {
    for (String currentSubnet : subnets) {
      // if a subnet name is given then get the region of that subnet based on the zone
      if (subnet != null && !currentSubnet.endsWith("subnetworks/" + subnet)) {
        continue;
      }
      if (currentSubnet.contains(region + "/subnetworks")) {
        return currentSubnet;
      }
    }
    throw new IllegalArgumentException(
      String.format("Could not find %s in network '%s' that are for region '%s'", subnet == null ? "any subnet" :
        String.format("a subnet named '%s", subnet), network, region));
  }

  private Network getNetworkInfo(String project, String network, Compute compute)
    throws IOException, RetryableProvisionException {
    Network networkObj;
    try {
      networkObj = compute.networks().get(project, network).execute();
    } catch (Exception e) {
      handleRetryableExceptions(e);
      throw e;
    }

    if (networkObj == null) {
      throw new IllegalArgumentException(String.format("Unable to find network '%s' in project '%s'. "
                                                         + "Please specify another network.", network, project));
    }
    return networkObj;
  }

  /**
   * Determines if the Dataproc cluster is private IP only.
   *
   * @param privateInstance a system config to force using private instance
   * @return {@code true} for pribvate IP only Dataproc cluster
   */
  private boolean isInternalIPOnly(Network network, boolean privateInstance) {
    String systemProjectId = null;
    String systemNetwork = null;
    try {
      systemProjectId = DataprocUtils.getSystemProjectId();
      systemNetwork = DataprocUtils.getSystemNetwork();
    } catch (IllegalArgumentException e) {
      // expected when not running on GCP, ignore
    }

    // Use private IP only cluster if privateInstance is true or if the compute profile required
    if (!privateInstance && conf.isPreferExternalIP()) {
      return false;
    }

    // If it is forced to be private instance or
    // if CDAP runs in GCP project and runtime job manager is used and monitoring is not done through SSH,
    // then we don't need to validate network connectivity
    if (privateInstance) {
      return true;
    }

    // If the CDAP is not running on GCP VM, then we just honor the prefer external IP config
    if (systemProjectId == null || systemNetwork == null) {
      return true;
    }

    // SSH will be used for job launching and/or monitoring, we need to validate network connectivity
    // CDAP and Dataproc are in the same network, should be able to use private IP only cluster
    if (systemProjectId.equals(conf.getNetworkHostProjectID()) && systemNetwork.equals(network.getName())) {
      return true;
    }

    // Check network is peering, we can use private ip only cluster
    PeeringState state = getPeeringState(systemProjectId, systemNetwork, network);
    if (state == PeeringState.ACTIVE) {
      return true;
    }

    // If there is no network connectivity and yet private ip only cluster is requested, raise an exception
    throw new DataprocRuntimeException(
      String.format("Direct network connectivity is needed for private Dataproc cluster between VPC %s/%s and %s/%s",
                    systemProjectId, systemNetwork, conf.getNetworkHostProjectID(), network.getName())
    );
  }

  /**
   * Finds ingress firewall rules for the configured network that matches the required firewall port as
   * defined in {@link FirewallPort}.
   *
   * @return a {@link Collection} of tags that need to be added to the VM to have those firewall rules applies
   * @throws IOException If failed to discover those firewall rules
   */
  private List<String> getFirewallTargetTags(Network network, boolean useInternalIP)
    throws IOException, RetryableProvisionException {
    FirewallList firewalls;
    try {
      firewalls = compute.firewalls().list(conf.getNetworkHostProjectID()).execute();
    } catch (Exception e) {
      handleRetryableExceptions(e);
      throw e;
    }

    List<String> tags = new ArrayList<>();
    Set<FirewallPort> requiredPorts = EnumSet.allOf(FirewallPort.class);
    // Iterate all firewall rules and see if it has ingress rules for all required firewall port.
    for (Firewall firewall : Optional.ofNullable(firewalls.getItems()).orElse(Collections.emptyList())) {
      // network is a url like https://www.googleapis.com/compute/v1/projects/<project>/<region>/networks/<name>
      // we want to get the last section of the path and compare to the configured network name
      int idx = firewall.getNetwork().lastIndexOf('/');
      String networkName = idx >= 0 ? firewall.getNetwork().substring(idx + 1) : firewall.getNetwork();
      if (!networkName.equals(network.getName())) {
        continue;
      }

      String direction = firewall.getDirection();
      if (!"INGRESS".equals(direction) || firewall.getAllowed() == null) {
        continue;
      }

      if (useInternalIP) {
        // If the Dataproc cluster is using internal IP only, we are only interested in firewall rule that has source
        // IP range overlap with one of the private IP block or doesn't have source IP at all.
        // This is because if Dataproc cluster is using internal IP, the CDAP itself must be running inside one of the
        // private IP blocks in order to be able to communicate with Dataproc.
        try {
          List<IPRange> sourceRanges = Optional.ofNullable(firewall.getSourceRanges())
            .map(DataprocUtils::parseIPRanges)
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
        } else if ("tcp".equalsIgnoreCase(protocol) && isPortAllowed(allowed.getPorts(), FirewallPort.SSH.port)) {
          requiredPorts.remove(FirewallPort.SSH);
          addTag = true;
        }
        if (addTag && firewall.getTargetTags() != null && !firewall.getTargetTags().isEmpty()) {
          tags.add(firewall.getTargetTags().iterator().next());
        }
      }
    }

    if (!requiredPorts.isEmpty()) {
      String portList = requiredPorts.stream().map(p -> String.valueOf(p.port)).collect(Collectors.joining(","));
      throw new IllegalArgumentException(String.format(
        "Could not find an ingress firewall rule for network '%s' in project '%s' for ports '%s'. " +
          "Please create a rule to allow incoming traffic on those ports for your IP range.",
        network.getName(), conf.getNetworkHostProjectID(), portList));
    }
    return tags;
  }

  /**
   * Returns if the given port is allowed by the list of allowed ports. The allowed ports is in format as allowed by
   * GCP firewall rule.
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

  @Override
  public void close() {
    client.close();
  }

  private enum PeeringState {
    ACTIVE,
    INACTIVE,
    NONE
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
