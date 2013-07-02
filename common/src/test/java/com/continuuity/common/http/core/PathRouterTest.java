package com.continuuity.common.http.core;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 *  Test the routing logic using String as the destination.
 */
public class PathRouterTest {

  @Test
  public void testPathRoutings(){

    PatternPathRouterWithGroups<String> pathRouter = new PatternPathRouterWithGroups();
    pathRouter.add("/foo/{baz}/b", "foobarb");
    pathRouter.add("/foo/bar/baz", "foobarbaz");
    pathRouter.add("/baz/bar", "bazbar");
    pathRouter.add("/bar", "bar");
    pathRouter.add("/foo/bar", "foobar");
    pathRouter.add("//multiple/slash//route", "multipleslashroute");

    Map<String, String> emptyGroupValues = Maps.newHashMap();
    Map<String, String> groupValues = Maps.newHashMap();

    List<String> routes = Lists.newArrayList();

    routes = pathRouter.getDestinations("/foo/bar/baz", emptyGroupValues);
    assertEquals(1, routes.size());
    assertEquals("foobarbaz", routes.get(0));
    assertEquals(0, emptyGroupValues.size());

    routes = pathRouter.getDestinations("/baz/bar", emptyGroupValues);
    assertEquals(1, routes.size());
    assertEquals("bazbar", routes.get(0));

    routes = pathRouter.getDestinations("/foo/bar/baz/moo", emptyGroupValues);
    assertEquals(0, routes.size());

    routes = pathRouter.getDestinations("/bar/121", emptyGroupValues);
    assertEquals(0, routes.size());

    routes = pathRouter.getDestinations("/foo/bar/b", groupValues);
    assertEquals(1, routes.size());
    assertEquals("foobarb", routes.get(0));
    assertEquals(1, groupValues.size());
    assertEquals("bar", groupValues.get("baz"));

    routes = pathRouter.getDestinations("/foo/bar", emptyGroupValues);
    assertEquals(1, routes.size());
    assertEquals("foobar", routes.get(0));

    routes = pathRouter.getDestinations("/multiple/slash/route", emptyGroupValues);
    assertEquals(1, routes.size());
    assertEquals("multipleslashroute", routes.get(0));


    routes = pathRouter.getDestinations("/foo/bar/bazooka", emptyGroupValues);
    assertEquals(0, routes.size());
  }
}
