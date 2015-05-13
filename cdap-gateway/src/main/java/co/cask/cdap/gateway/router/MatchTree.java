/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.router;

import co.cask.cdap.common.utils.ImmutablePair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A decision tree structure to match routes by HTTP method and URI path.
 */
class MatchTree {

  public static final String USER_SERVICE_FORMAT = "service.%s.%s.%s";
  public static final String NO_ROUTE = "";

  private Map<String, MatchTree> byPath = Maps.newHashMap();
  private Map<HttpMethod, ImmutablePair<String, Integer>> byMethod = Maps.newHashMap();
  private MatchTree pathWildcard = null;
  private String methodWildcard = null;

  public String match(HttpMethod method, String path) {
    String[] pathParts = StringUtils.split(path, '/');
    String service = match(method, pathParts, 0);
    // This is a hack - we only want to format in this single case.
    // This performs better than checking whether the service string contains a %, and then applying the format().
    if (USER_SERVICE_FORMAT.equals(service)) {
      service = String.format(USER_SERVICE_FORMAT, pathParts[2], pathParts[4], pathParts[6]);
    }
    return service;
  }

  private String match(HttpMethod method, String[] pathParts, int pos) {
    if (pos < pathParts.length) {
      MatchTree next = byPath.get(pathParts[pos]);
      if (next == null) {
        next = pathWildcard;
      }
      if (next != null) {
        return next.match(method, pathParts, pos + 1);
      }
    }
    ImmutablePair<String, Integer> serviceEntry = byMethod.get(method);
    if (serviceEntry == null) {
       return methodWildcard;
    }
    return serviceEntry.getFirst();
  }

  public void addRoute(HttpMethod httpMethod, String path, String service) throws ConflictingRouteException {
    addRoute(httpMethod, StringUtils.split(path, '/'), 0, service, null);
  }

  private void addRoute(HttpMethod httpMethod, String[] pathParts, int pos, String service, Integer firstWildcard)
    throws ConflictingRouteException {

    if (pathParts.length <= pos) {
      // traversed whole path, now deal with method
      enterMethod(httpMethod, service, firstWildcard);
      return;
    }
    String part = pathParts[pos];
    if (part.startsWith("{") && part.endsWith("}")) { // path variable
      part = null;
    }

    for (ImmutablePair<MatchTree, Boolean> next : enterPath(part)) {
      next.getFirst().addRoute(httpMethod, pathParts, pos + 1, service,
                               next.getSecond() || part != null ? firstWildcard : ((Integer) pos));
    }
  }

  private Collection<ImmutablePair<MatchTree, Boolean>> enterPath(String pathPart) {
    List<ImmutablePair<MatchTree, Boolean>> nodes = Lists.newLinkedList();
    if (pathPart == null) {
      for (MatchTree node : byPath.values()) {
        nodes.add(ImmutablePair.of(node, false));
      }
      if (pathWildcard == null) {
        pathWildcard = new MatchTree();
      }
      nodes.add(ImmutablePair.of(pathWildcard, true));
      return nodes;
    }
    // not wildcard
    MatchTree node = byPath.get(pathPart);
    if (node == null) {
      if (pathWildcard != null) {
        node = pathWildcard.deepCopy();
      } else {
        node = new MatchTree();
      }
      byPath.put(pathPart, node);
    }
    return Collections.singletonList(ImmutablePair.of(node, false));
  }

  private void enterMethod(HttpMethod method, String service, Integer firstWildcard) throws ConflictingRouteException {
    if (method == null) {
      if (methodWildcard != null) {
        throw new ConflictingRouteException(String.format(
          "Attempt to add service '%s' for all methods which is already bound to service '%s' for all methods",
          service, methodWildcard));
      }
      methodWildcard = service;
      return;
    }
    ImmutablePair<String, Integer> existing = byMethod.get(method);
    if (existing != null) {
      if (existing.getFirst().equals(service)) {
        return;
      }
      if ((existing.getSecond() == null && firstWildcard != null)
        || (existing.getSecond() != null && firstWildcard != null && firstWildcard < existing.getSecond())) {
        return;
      }
      if (Objects.equals(existing.getSecond(), firstWildcard)) {
        throw new ConflictingRouteException(String.format(
          "Attempt to add service '%s' for method %s which is already bound to service '%s'",
          service, method, existing));
      }
    }
    byMethod.put(method, ImmutablePair.of(service, firstWildcard));
  }

  public void optimize() {
    for (MatchTree next : byPath.values()) {
      next.optimize();
    }
    if (pathWildcard != null) {
      pathWildcard.optimize();
    }
    // if all methods return the same service, replace with wildcard
    Set<String> services = Sets.newHashSet();
    for (ImmutablePair<String, Integer> entry : byMethod.values()) {
      services.add(entry.getFirst());
    }
    if (methodWildcard != null) {
      services.add(methodWildcard);
    }
    if (services.size() == 1) {
      methodWildcard = services.iterator().next();
      byMethod.clear();
    }
    Set<String> allServices = services();
    if (allServices.size() == 1) {
      byPath.clear();
      pathWildcard = null;
      byMethod.clear();
      methodWildcard = allServices.iterator().next();
    }

    if (pathWildcard != null) {
      for (String path : ImmutableList.copyOf(byPath.keySet())) {
        if (byPath.get(path).equals(pathWildcard)) {
          byPath.remove(path);
        }
      }
    } else if (methodWildcard != null) {
      for (String path : ImmutableList.copyOf(byPath.keySet())) {
        Set<String> srvcs = byPath.get(path).services();
        if (srvcs.size() == 1 && srvcs.iterator().next().equals(methodWildcard)) {
          byPath.remove(path);
        }
      }
    }
  }

  private Set<String> services() {
    Set<String> set = Sets.newHashSet();
    for (MatchTree next : byPath.values()) {
      set.addAll(next.services());
    }
    if (pathWildcard != null) {
      set.addAll(pathWildcard.services());
    }
    for (ImmutablePair<String, Integer> entry : byMethod.values()) {
      set.add(entry.getFirst());
    }
    if (methodWildcard != null) {
      set.add(methodWildcard);
    }
    return set;
  }

  private MatchTree deepCopy() {
    MatchTree copy = new MatchTree();
    if (pathWildcard != null) {
      copy.pathWildcard = pathWildcard.deepCopy();
    }
    copy.methodWildcard = methodWildcard;
    for (Map.Entry<String, MatchTree> entry : byPath.entrySet()) {
      copy.byPath.put(entry.getKey(), entry.getValue().deepCopy());
    }
    for (Map.Entry<HttpMethod, ImmutablePair<String, Integer>> entry : byMethod.entrySet()) {
      copy.byMethod.put(entry.getKey(), entry.getValue());
    }
    return copy;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (null == o || o.getClass() != MatchTree.class) {
      return false;
    }
    MatchTree other = (MatchTree) o;
    return Objects.equals(pathWildcard, other.pathWildcard)
      && Objects.equals(methodWildcard, other.methodWildcard)
      && Objects.equals(byMethod, other.byMethod)
      && Objects.equals(byPath, other.byPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pathWildcard, methodWildcard, byMethod, byPath);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    buildString(builder, 0);
    return builder.toString().replaceAll("\n+", "\n");
  }

  private void buildString(StringBuilder builder, int indent) {
    boolean first = true;
    for (Map.Entry<String, MatchTree> entry : byPath.entrySet()) {
      first = addEntry(builder, first, indent, entry.getKey(), entry.getValue());
    }
    if (pathWildcard != null) {
      first = addEntry(builder, first, indent, "{...}", pathWildcard);
    }
    for (Map.Entry<HttpMethod, ImmutablePair<String, Integer>> entry : byMethod.entrySet()) {
      first = addMethod(builder, first, indent, entry.getKey().toString(), entry.getValue().getFirst());
    }
    if (methodWildcard != null) {
      addMethod(builder, first, indent, "ALL", methodWildcard);
    }
  }

  private boolean addEntry(StringBuilder builder, boolean first, int indent, String key, MatchTree next) {
    indent(builder, first, indent);
    builder.append('/').append(key);
    next.buildString(builder, indent + 1 + key.length());
    builder.append('\n');
    return false;
  }

  private boolean addMethod(StringBuilder builder, boolean first, int indent, String method, String service) {
    indent(builder, first, indent);
    builder.append('@').append(method).append("->").append(service).append('\n');
    return false;
  }

  private static void indent(StringBuilder builder, boolean first, int indent) {
    if (!first) {
      for (int i = 0; i < indent; i++) {
        builder.append(' ');
      }
    }
  }

}
