/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http.core;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Matches incoming un-matched paths to destinations. Designed to be used for routing URI paths to http resources.
 * Parameters within braces "{}" are treated as template parameter (a named wild-card pattern).
 * @param <T> represents the destination of the routes.
 */
public class PatternPathRouterWithGroups<T> {

  private final Multimap<Pattern, RouteDestinationWithGroups<T>> patternMap;
  private final Pattern groupPattern;

  /**
   * Initialize PatternPathRouterWithGroups.
   */
  public PatternPathRouterWithGroups(){
    this.patternMap = ArrayListMultimap.create();
    this.groupPattern = Pattern.compile("\\{(.*?)\\}");
  }

  /**
   * Add a source and destination.
   * @param source  Source path to be routed. Routed path can have named wild-card pattern with braces "{}".
   * @param destination Destination of the path.
   */
  public void add(final String source, final T destination){
    String path = (source.endsWith("/")) ? source.substring(0, source.length() - 1) :
                                           source;

    String [] parts = path.split("/");
    StringBuilder sb =  new StringBuilder();
    List<String> groupNames = Lists.newArrayList();

    for (String part : parts){
      Matcher matcher = groupPattern.matcher(part);
      if (matcher.matches()) {
        groupNames.add(matcher.group(1));
        sb.append("([^/]+?)");
      } else {
        sb.append(part);
      }
      sb.append("/");
    }

    //Removes the last "/"
    sb.deleteCharAt(sb.length() - 1);

    Pattern pattern = Pattern.compile(sb.toString());
    patternMap.put(pattern, new RouteDestinationWithGroups<T>(destination, groupNames));
  }

  /**
   * Get a list of destinations and the values matching templated parameter for the given path.
   * Returns an empty list when there are no destinations that are matched.
   * @param path path to be routed.
   * @param groupNameValues Map of templated parameter and string representation group value matching the
   *                        templated parameter as the value.
   * @return List of Destinations matching the given route.
   */
  public List<T> getDestinations(final String path, final Map<String, String> groupNameValues){

    List<T> result = Lists.newArrayList();
    for (Map.Entry<Pattern, RouteDestinationWithGroups<T>> entry : patternMap.entries()){
      Matcher matcher = entry.getKey().matcher(path);
      if (matcher.matches()){
        int matchIndex = 1;
        for (String name : entry.getValue().getGroupNames()){
          String value = matcher.group(matchIndex);
          groupNameValues.put(name, value);
          matchIndex++;
        }
        result.add(entry.getValue().getDestination());
      }
    }
    return result;
  }

  /**
   * Helper class to store the groupNames and Destination.
   * @param <T> Destination.
   */
  private class RouteDestinationWithGroups<T> {

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
}
