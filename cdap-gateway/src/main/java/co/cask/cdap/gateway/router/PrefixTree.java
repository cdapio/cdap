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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A decision tree structure to match routes by URI path.
 */
class PrefixTree {

  private Map<String, PrefixTree> byPath = Maps.newHashMap();
  private PrefixTree defaultPath = null;
  private Match match;

  public String match(String path) {
    String[] pathParts = StringUtils.split(path, '/');
    Match match = match(pathParts, 0);
    if (match == null) {
      return null;
    }
    String service = match.getService();
    // This is a hack - we only want to format in this single case.
    // This performs better than checking whether the service string contains a %, and then applying the format().
    if (RouterPathLookup.USER_SERVICE_FORMAT.equals(service)) {
      service = String.format(RouterPathLookup.USER_SERVICE_FORMAT, pathParts[2], pathParts[4], pathParts[6]);
    }
    return service;
  }

  public Match match(String[] parts, int pos) {
    if (pos >= parts.length) {
      // no more parts in the input -> return current match
      return match;
    }
    String part = parts[pos];
    PrefixTree next = byPath.get(part);
    next = next == null ? defaultPath : next;
    if (next == null) {
      // nothing to follow, return current match
      return match;
    }
    return next.match(parts, pos + 1);
  }

  public void addRoute(String path, String service, Route route) throws ConflictingRouteException {
    addRoute(StringUtils.split(path, '/'), 0, service, route, "");
  }

  private void addRoute(String[] pathParts, int pos, String service, Route route, String wildcards)
    throws ConflictingRouteException {

    if (pos >= pathParts.length) {
      // traversed whole path, merge the new match into this node
      Match newMatch = new Match(service, wildcards, route);
      match = match == null ? newMatch : match.select(newMatch);
      return;
    }
    String part = pathParts[pos];
    boolean wildcard = part.startsWith("{") && part.endsWith("}"); // path variable
    if (wildcard) {
      // wildcard must be added to all routes, be it by path or default
      for (PrefixTree next : byPath.values()) {
        next.addRoute(pathParts, pos + 1, service, route, wildcards + '*');
      }
      defaultPath = defaultPath != null ? defaultPath : new PrefixTree();
      defaultPath.addRoute(pathParts, pos + 1, service, route, wildcards + '*');
    } else {
      PrefixTree next = byPath.get(part);
      if (next == null) {
        next = defaultPath != null ? defaultPath.deepCopy() : new PrefixTree();
        byPath.put(part, next);
      }
      next.addRoute(pathParts, pos + 1, service, route, wildcards + ' ');
    }
  }

  private PrefixTree deepCopy() {
    PrefixTree copy = new PrefixTree();
    if (match != null) {
      copy.match = new Match(match);
    }
    if (defaultPath != null) {
      copy.defaultPath = defaultPath.deepCopy();
    }
    for (Map.Entry<String, PrefixTree> entry : byPath.entrySet()) {
      copy.byPath.put(entry.getKey(), entry.getValue().deepCopy());
    }
    return copy;
  }

  public void optimize() {
    // first optimize all subtrees
    for (PrefixTree next : byPath.values()) {
      next.optimize();
    }
    if (defaultPath != null) {
      defaultPath.optimize();
    }
    // 1. if all branches yield the same service, just match that service at this node
    Match singleMatch = isSingleService();
    if (singleMatch != null) {
      byPath.clear();
      defaultPath = null;
      match = singleMatch;
      return;
    }
    Match defaultMatch = null;
    if (defaultPath != null) {
      defaultMatch = defaultPath.isSingleService();
      if (defaultMatch != null) {
        if (match == null) {
          // 2. if there is no match at this node but there is a default path that yields only one service,
          //    then we can just match that same service at this node, and we don't need the default path.
          match = defaultMatch;
          defaultPath = null;
        } else if (match.sameService(defaultMatch)) {
          // 3. if the default path can only yield the same service as the match at this node, then we don't need it
          match.addRoutes(defaultMatch);
          defaultMatch = match;
          defaultPath = null;
        }
      }
    } else if (match != null) {
      defaultMatch = match;
    }
    // 4. If there is a default path that yields only one service, or if there is no default path but a match at this
    //    node, then we can remove all subtrees that only yield that same service - because they will default to it.
    if (defaultMatch != null) {
      List<Route> routes = Lists.newLinkedList();
      for (String key : ImmutableSet.copyOf(byPath.keySet())) {
        if (byPath.get(key).isSameService(defaultMatch, routes)) {
          byPath.remove(key);
        }
      }
      defaultMatch.addRoutes(routes);
    }
  }

  /**
   * Determine whether all branches under this node match the same service.
   * @return The shortest prefix match for that service within this tree.
   *         The returned match will contain the routes of all other matches found.
   */
  private Match isSingleService() {
    Match result = null;
    List<Route> routes = Lists.newLinkedList();
    if (match != null) {
      result = new Match(match);
    }
    if (result == null) {
      if (defaultPath != null) {
        result = defaultPath.isSingleService();
        if (result == null) {
          return null;
        }
      }
    } else {
      if (defaultPath != null && !defaultPath.isSameService(result, routes)) {
        return null;
      }
    }
    for (PrefixTree next : byPath.values()) {
      if (result == null) {
        result = next.isSingleService();
        if (result == null) {
          return null;
        }
      } else {
        if (!next.isSameService(result, routes)) {
          return null;
        }
      }
    }
    if (result != null) {
      result.addRoutes(routes);
    }
    return result;
  }

  /**
   * Determine whether all branches under this node match the given match's service, and
   * add the routes of all matches to the provided list.
   */
  private boolean isSameService(Match otherMatch, List<Route> routes) {
    if (match != null) {
      if (match.sameService(otherMatch)) {
        routes.addAll(match.getRoutes());
      } else {
        return false;
      }
    }
    if (defaultPath != null && !defaultPath.isSameService(otherMatch, routes)) {
      return false;
    }
    for (PrefixTree next : byPath.values()) {
      if (!next.isSameService(otherMatch, routes)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return toString(false);
  }

  public String toString(boolean verbose) {
    StringBuilder builder = new StringBuilder();
    buildString(builder, 0, verbose);
    return builder.toString().replaceAll("\n+", "\n");
  }

  private void buildString(StringBuilder builder, int indent, boolean verbose) {
    boolean first = true;
    for (Map.Entry<String, PrefixTree> entry : byPath.entrySet()) {
      first = addEntry(builder, first, indent, verbose, entry.getKey(), entry.getValue());
    }
    if (defaultPath != null) {
      first = addEntry(builder, first, indent, verbose, "{...}", defaultPath);
    }
    if (match != null) {
      addMatch(builder, first, indent, verbose, match);
      builder.append('\n');
    }
  }

  private boolean addEntry(StringBuilder builder, boolean first, int indent, boolean verbose,
                           String key, PrefixTree next) {
    indent(builder, first, indent);
    builder.append('/').append(key);
    next.buildString(builder, indent + 1 + key.length(), verbose);
    builder.append('\n');
    return false;
  }

  private void addMatch(StringBuilder builder, boolean first, int indent, boolean verbose, Match match) {
    indent(builder, first, indent);
    builder.append(match.toString(verbose));
  }

  private static void indent(StringBuilder builder, boolean first, int indent) {
    if (!first) {
      for (int i = 0; i < indent; i++) {
        builder.append(' ');
      }
    }
  }

  /**
   * Represents a prefix match in a path. It contains the service that is matched, a wildcard string to help resolve
   * conflicts when two paths overlap, and the list of all routes that are represented by this match.
   */
  private static class Match {
    private final String service;
    private final String wildcards;
    private final List<Route> routes = Lists.newLinkedList();

    public Match(String service, String wildcards, Route route) {
      this.service = service;
      this.wildcards = wildcards;
      this.routes.add(route);
    }

    public Match(Match other) {
      this.service = other.service;
      this.wildcards = other.wildcards;
      this.routes.addAll(other.routes);
    }

    public String getService() {
      return service;
    }

    public List<Route> getRoutes() {
      return routes;
    }

    public boolean sameService(Match other) {
      return sameService(other.getService());
    }

    private boolean sameService(String otherService) {
      return service.equals(otherService);
    }

    public void addRoutes(Match other) {
      this.routes.addAll(other.routes);
    }

    public void addRoutes(Collection<Route> toAdd) {
      this.routes.addAll(toAdd);
    }

    /**
     * Select the more specific one from two matches if they do not conflict. A conflict exists if
     * both matches are equally specificbut return differenty serives.
     *
     * @return the more specific one out of this and the other match
     * @throws ConflictingRouteException if the other match is in conflict
     */
    public Match select(Match other) throws ConflictingRouteException {
      int wildcardComp = wildcardCompare(wildcards, other.wildcards);
      if (wildcardComp != 0 || service.equals(other.service)) {
        if (wildcardComp <= 0) {
          this.routes.addAll(other.routes);
          return this;
        } else {
          other.routes.addAll(this.routes);
          return other;
        }
      }
      // conflict: both matches are equally specific, yet return a different service
      throw new ConflictingRouteException(String.format(
        "Conflict between routes %s and %s which are both equally specific", this, other));
    }

    /**
     * Determine which of two wildcard strings is more specific. A wildcard string is the more specific the
     * later the first wildcard character occurs in it. For route matches, a more specific match prevails.
     * Wildcard strings are sequences of blanks and '*'. A blank in position i indicates that the ith path
     * component is a wildcard (that is, a path variable). Hence, "  *" is more specific than "   ", and
     * " *  " is more specific than " * *". Right now, we only support comparing strings of the same length,
     * and will throw an exception otherwise.
     *
     * @return -1 if a is more specific than b, 0 if they are equally specific, and 1 if b is more specific.
     */
    private static int wildcardCompare(String a, String b) {
      Preconditions.checkArgument(a.length() == b.length());
      return a.compareTo(b);
    }

    @Override
    public String toString() {
      return toString(false);
    }

    public String toString(boolean verbose) {
      if (verbose) {
        return String.format(" -> %s %s", service, routes);
      } else {
        return String.format(" -> %s", service);
      }
    }
  }

  /**
   * Represents a route represented by a match: Its handler and method name,
   * and the HTTP method and @Path it is annotated with.
   */
  public static class Route {
    private final String handler;
    private final String methodName;
    private final String httpMethod;
    private final String path;

    public Route(String handler, String methodName, HttpMethod httpMethod, String path) {
      this(handler, methodName, httpMethod.getName(), path);
    }

    public Route(String handler, String methodName, String httpMethod, String path) {
      this.handler = handler;
      this.methodName = methodName;
      this.httpMethod = httpMethod;
      this.path = path;
    }

    @Override
    public String toString() {
      return String.format("%s %s [%s.%s]", httpMethod, path, handler, methodName);
    }
  }
}
