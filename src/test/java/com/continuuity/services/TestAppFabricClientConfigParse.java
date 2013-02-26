package com.continuuity.services;

import com.continuuity.app.services.AppFabricClient;
import com.continuuity.common.conf.CConfiguration;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Test app-fabric command parsing
 */
public class TestAppFabricClientConfigParse {

  @Test
  public void testOptionsParsing() throws ParseException {
    String[] args = {"deploy", "-archive", "jar"};
    AppFabricClient client = new AppFabricClient();
    client.configure(CConfiguration.create(), args);
    assert (client != null);
    assertTrue("deploy".equals(client.getCommand()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnknownCommands() throws ParseException {
    AppFabricClient client = new AppFabricClient();
    String command = client.configure(CConfiguration.create(), new String[]{"Foobaz", "-jar", "jar"});
  }

  @Test (expected = IllegalArgumentException.class)
  public void testValidInvalidDeployArgs() throws ParseException {
    AppFabricClient client = new AppFabricClient();
    String command = client.configure(CConfiguration.create(), new String[]{"deploy"});
    assert (command == null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidInvalidStartArgs() throws ParseException {
    AppFabricClient client = new AppFabricClient();
    String command = client.configure(CConfiguration.create(), new String[]{"SomeRandomCommand", "--application", "args"});
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidInvalidStopArgs() throws ParseException {
    AppFabricClient client = new AppFabricClient();
    String command = client.configure(CConfiguration.create(), new String[]{"stop", "--application", "args"});

  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidInvalidStatusArgs() throws ParseException {
    AppFabricClient client = new AppFabricClient();
    String command = null;
    command = client.configure(CConfiguration.create(), new String[]{"status", "--application", "args"});
 }

  @Test
  public void testValidInvalidPromoteArgs() throws ParseException {
    AppFabricClient client = new AppFabricClient();
    String command = null;
    command = client.configure(CConfiguration.create(), new String[]{"promote", "--host", "hostname",
      "--application", "application"});
    assert (command == null);
  }

  @Test
  public void testValidArguments() throws ParseException {

    AppFabricClient client = new AppFabricClient();
    String command = null;
    command = client.configure(CConfiguration.create(), new String[]{"deploy", "--archive", "jar"});
    assertTrue("deploy".equals(command));

    command = client.configure(CConfiguration.create(), new String[]{"start", "--application", "appId",
      "--procedure", "processor"});
    assertTrue("start".equals(command));


    command = client.configure(CConfiguration.create(), new String[]{"start", "--application", "appId",
      "--flow", "processor"});
    assertTrue("start".equals(command));


    command = client.configure(CConfiguration.create(), new String[]{"stop", "--application", "appId",
      "--procedure", "processor"});
    assertTrue("stop".equals(command));

    command = client.configure(CConfiguration.create(), new String[]{"stop", "--application", "appId",
      "--flow", "processor"});
    assertTrue("stop".equals(command));


    command = client.configure(CConfiguration.create(), new String[]{"status", "--application", "appId",
      "--procedure", "processor"});
    assertTrue("status".equals(command));
    command = client.configure(CConfiguration.create(), new String[]{"status", "--application", "appId",
      "--flow", "processor"});
    assertTrue("status".equals(command));

    command = client.configure(CConfiguration.create(), new String[]{"promote", "--hostname", "vpc_name",
      "--apikey", "Auth token",
      "--application", "application"});
    assertTrue("promote".equals(command));

  }
}
