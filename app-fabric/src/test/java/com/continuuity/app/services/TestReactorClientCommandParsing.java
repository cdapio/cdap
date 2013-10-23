package com.continuuity.app.services;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.UsageException;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test Reactor Client command parsing.
 */
public class TestReactorClientCommandParsing {

  @Test
  public void testOptionsParsing() throws ParseException {
    String[] args = {"deploy", "-archive", "jar"};
    ReactorClient client = new ReactorClient();
    client.parseArguments(args, CConfiguration.create());
    assertNotNull(client);
    assertTrue("deploy".equals(client.getCommand()));
  }

  public void testUnknownCommands() throws ParseException {
    ReactorClient client = new ReactorClient();
    String command = client.parseArguments(new String[]{"Foobaz", "-jar", "jar"}, CConfiguration.create());
    assertEquals("help", command);
  }

  @Test(expected = UsageException.class)
  public void testValidInvalidDeployArgs() throws ParseException {
    ReactorClient client = new ReactorClient();
    client.parseArguments(new String[]{"deploy"}, CConfiguration.create());
  }

  @Test(expected = UsageException.class)
  public void testValidInvalidDeleteArgs() throws ParseException {
    ReactorClient client = new ReactorClient();
    client.parseArguments(new String[]{"delete"}, CConfiguration.create());
  }

  public void testValidInvalidStartArgs() throws ParseException {
    ReactorClient client = new ReactorClient();
    String command = client.parseArguments(new String[]{"SomeRandomCommand", "--app", "args"},
                                           CConfiguration.create());
    assertEquals("help", command);
  }

  @Test(expected = UsageException.class)
  public void testValidInvalidStopArgs() throws ParseException {
    ReactorClient client = new ReactorClient();
    client.parseArguments(new String[]{"stop", "--app", "args"}, CConfiguration.create());

  }

  @Test(expected = UsageException.class)
  public void testValidInvalidStatusArgs() throws ParseException {
    ReactorClient client = new ReactorClient();
    client.parseArguments(new String[]{"status", "--app", "args"}, CConfiguration.create());
  }

  @Test(expected = UsageException.class)
  public void testValidInvalidPromoteArgs() throws ParseException {
    ReactorClient client = new ReactorClient();
    client.parseArguments(new String[]{"promote", "--remote", "host", "--app", "application"},
                          CConfiguration.create());
  }

  @Test
  public void testValidArguments() throws ParseException {
    ReactorClient client = new ReactorClient();
    assertTrue("help".equals(client.parseArguments(new String[]{"help"}, CConfiguration.create())));

    assertTrue("delete".equals(client.parseArguments(new String[]{"delete", "--app", "appId"},
                                                     CConfiguration.create())));

    assertTrue("deploy".equals(client.parseArguments(new String[]{"deploy", "--archive", "jar"},
                                                     CConfiguration.create())));

    assertTrue("start".equals(client.parseArguments(
      new String[]{"start", "--app", "appId", "--procedure", "processor"}, CConfiguration.create())));

    assertTrue("start".equals(client.parseArguments(
      new String[]{"start", "--app", "appId", "--flow", "processor"}, CConfiguration.create())));

    assertTrue("start".equals(client.parseArguments(
      new String[]{"start", "--app", "appId", "--flow", "processor", "--host", "localhost"},
      CConfiguration.create())));

    assertTrue("start".equals(client.parseArguments(
      new String[]{"start", "--app", "appId", "--flow", "processor", "--host", "localhost", "-RV=1", "-RU=2"},
      CConfiguration.create())));


    assertTrue("stop".equals(client.parseArguments(
      new String[]{"stop", "--app", "appId", "--procedure", "processor"}, CConfiguration.create())));

    assertTrue("stop".equals(client.parseArguments(
      new String[]{"stop", "--app", "appId", "--flow", "processor"}, CConfiguration.create())));

    assertTrue("stop".equals(client.parseArguments(
      new String[]{"stop", "--app", "appId", "--flow", "processor", "--host", "localhost"},
      CConfiguration.create())));

    assertTrue("status".equals(client.parseArguments(
      new String[]{"status", "--app", "appId", "--procedure", "processor"}, CConfiguration.create())));

    assertTrue("status".equals(client.parseArguments(
      new String[]{"status", "--app", "appId", "--flow", "processor"}, CConfiguration.create())));

    assertTrue("promote".equals(client.parseArguments(
      new String[]{"promote", "--remote", "vpc_name", "--apikey", "Auth token", "--app", "application"},
      CConfiguration.create())));

    assertTrue("scale".equals(client.parseArguments(
      new String[]{"scale", "--app", "appId", "--flow", "processor", "--flowlet", "count", "--instances", "3"},
      CConfiguration.create())));
  }

  @Test(expected = UsageException.class)
  public void testInvalidFlowletInstancesArgs() throws ParseException {
    ReactorClient client = new ReactorClient();
    client.parseArguments(new String[]{"scale", "--app", "appId", "--flow", "processor", "--flowlet",
      "count", "--instances", "0"}, CConfiguration.create());
  }
}
