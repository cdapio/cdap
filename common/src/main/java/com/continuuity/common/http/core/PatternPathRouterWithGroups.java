/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http.core;

import com.continuuity.common.utils.ImmutablePair;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Matches incoming un-matched paths to destinations. Designed to be used for routing URI paths to http resources.
 * Parameters within braces "{}" are treated as template parameter (a named wild-card pattern).
 *
 * @param <T> represents the destination of the routes.
 */
public final class PatternPathRouterWithGroups<T> {

  //GROUP_PATTERN is used for named wild card pattern in paths which is specified within braces.
  //Example: {id}
  private static final Pattern GROUP_PATTERN = Pattern.compile("\\{(.*?)\\}");

  // non-greedy wild card match.
  private static final Pattern WILD_CARD_PATTERN = Pattern.compile("\\*\\*");

  private final List<ImmutablePair<Pattern, RouteDestinationWithGroups<T>>> patternRouteList;

  /**
   * Initialize PatternPathRouterWithGroups.
   */
  public PatternPathRouterWithGroups(){
    this.patternRouteList = Lists.newArrayList();
  }

  /**
   * Add a source and destination.
   *
   * @param source  Source path to be routed. Routed path can have named wild-card pattern with braces "{}".
   * @param destination Destination of the path.
   */
  public void add(final String source, final T destination){

    // replace multiple slashes with a single slash.
    String path = source.replaceAll("/+", "/");

    path = (path.endsWith("/") && path.length() > 1)
      ? path.substring(0, path.length() - 1) : path;


    String [] parts = path.split("/");
    StringBuilder sb =  new StringBuilder();
    List<String> groupNames = Lists.newArrayList();

    for (String part : parts){
      Matcher groupMatcher = GROUP_PATTERN.matcher(part);
      if (groupMatcher.matches()) {
        groupNames.add(groupMatcher.group(1));
        sb.append("([^/]+?)");
      } else if (WILD_CARD_PATTERN.matcher(part).matches()) {
        sb.append(".*?");
      } else {
        sb.append(part);
      }
      sb.append("/");
    }

    //Ignore the last "/"
    sb.setLength(sb.length() - 1);

    Pattern pattern = Pattern.compile(sb.toString());
    patternRouteList.add(new ImmutablePair<Pattern,
      RouteDestinationWithGroups<T>>(pattern, new RouteDestinationWithGroups<T>(destination, groupNames)));
  }

  /**
   * Get a list of destinations and the values matching templated parameter for the given path.
   * Returns an empty list when there are no destinations that are matched.
   *
   * @param path path to be routed.
   * @return List of Destinations matching the given route.
   */
  public List<RoutableDestination<T>> getDestinations(String path){

    String cleanPath = (path.endsWith("/") && path.length() > 1)
      ? path.substring(0, path.length() - 1) : path;

    List<RoutableDestination<T>> result = Lists.newArrayList();

    for (ImmutablePair<Pattern, RouteDestinationWithGroups<T>> patternRoute : patternRouteList) {
      ImmutableMap.Builder<String, String> groupNameValuesBuilder = ImmutableMap.builder();
      Matcher matcher =  patternRoute.getFirst().matcher(cleanPath);
      if (matcher.matches()){
        int matchIndex = 1;
        for (String name : patternRoute.getSecond().getGroupNames()){
          String value = matcher.group(matchIndex);
          groupNameValuesBuilder.put(name, value);
          matchIndex++;
        }
        result.add(new RoutableDestination<T>(patternRoute.getSecond().getDestination(),
                                              groupNameValuesBuilder.build()));
      }
    }
    return result;
  }

  /**
   * Helper class to store the groupNames and Destination.
   *
   * @param <T> Destination.
   */
  private final class RouteDestinationWithGroups<T> {

    private final T destination;
    private final List<String> groupNames;

    public RouteDestinationWithGroups (T destination, List<String> groupNames){
      this.destination = destination;
      this.groupNames = groupNames;
    }

    public T getDestination() {
      return destination;
    }

    public List<String> getGroupNames() {
      return groupNames;
    }
  }

  /**
   * Represents a matched destination.
   * @param <T> Type of destination.
   */
  public static final class RoutableDestination<T> {
    private final T destination;
    private final Map<String, String> groupNameValues;

    public RoutableDestination(T destination, Map<String, String> groupNameValues) {
      this.destination = destination;
      this.groupNameValues = groupNameValues;
    }

    public T getDestination() {
      return destination;
    }

    /**
     * @return Map of templated parameter and string representation group value matching the templated parameter as
     * the value.
     */
    public Map<String, String> getGroupNameValues() {
      return groupNameValues;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("destination", destination)
        .add("groupNameValues", groupNameValues)
        .toString();
    }
  }
}
