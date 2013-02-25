package com.continuuity.services;

import com.continuuity.app.services.AppFabricClient;
import com.continuuity.common.conf.CConfiguration;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Test app-fabric command parsing
 */
public class TestAppFabricParser {

  @Test
  public void testOptionsParsing() throws ParseException {
    String[] args = {"deploy", "-jar", "jar"};
    AppFabricClient client = new AppFabricClient();
    client.configure(CConfiguration.create(), args);
    assert (client != null);
    assertTrue("deploy".equals(client.getCommand()));
  }

  @Test
  public void testUnknownCommands() throws ParseException {
    AppFabricClient client = new AppFabricClient();
    String command = client.configure(CConfiguration.create(), new String[]{"Foobaz", "-jar", "jar"});
    assertTrue(command == null);
  }

  @Test
  public void testValidInvalidDeployArgs() throws ParseException {
    AppFabricClient client = new AppFabricClient();
    String command = null;
    command = client.configure(CConfiguration.create(), new String[]{"deploy"});
    assertTrue(command == null);
  }

  @Test
  public void testValidInvalidVerifyArgs() throws ParseException {
    AppFabricClient client = new AppFabricClient();
    String command = null;

    command = client.configure(CConfiguration.create(), new String[]{"verify", "--application", "args"});
    assertTrue(command == null);
  }

  @Test
  public void testValidInvalidStartArgs() throws ParseException {
    AppFabricClient client = new AppFabricClient();
    String command = null;
    command = client.configure(CConfiguration.create(), new String[]{"start", "--application", "args"});
    assertTrue(command == null);

    command = client.configure(CConfiguration.create(), new String[]{"start", "--processor", "args"});
    assertTrue(command == null);
  }

  @Test
  public void testValidInvalidStopArgs() throws ParseException {
    AppFabricClient client = new AppFabricClient();
    String command = null;

    command = client.configure(CConfiguration.create(), new String[]{"stop", "--application", "args"});
    assertTrue(command == null);

    command = client.configure(CConfiguration.create(), new String[]{"stop", "--processor", "args"});
    assertTrue(command == null);
  }


  @Test
  public void testValidInvalidStatusArgs() throws ParseException {
    AppFabricClient client = new AppFabricClient();
    String command = null;

    command = client.configure(CConfiguration.create(), new String[]{"status", "--application", "args"});
    assertTrue(command == null);

    command = client.configure(CConfiguration.create(), new String[]{"status", "--processor", "args"});
    assertTrue(command == null);
  }

  @Test
  public void testValidInvalidPromoteArgs() throws ParseException {
    AppFabricClient client = new AppFabricClient();
    String command = null;
    command = client.configure(CConfiguration.create(), new String[]{"promote", "--vpc", "vpc_name",
      "--application", "application"});
    assert (command == null);

    command = client.configure(CConfiguration.create(), new String[]{"promote", "--vpc", "vpc_name",
      "--application", "application"});
    assert (command == null);

    command = client.configure(CConfiguration.create(), new String[]{"promote",
      "--authtoken", "Auth token",
      "--application", "application"});
    assert (command == null);

  }

  @Test
  public void testValidArguments() throws ParseException {

    AppFabricClient client = new AppFabricClient();
    String command = null;
    command = client.configure(CConfiguration.create(), new String[]{"deploy", "--resource", "jar"});
    assertTrue("deploy".equals(command));

    command = client.configure(CConfiguration.create(), new String[]{"verify", "--resource", "jar"});
    assertTrue("verify".equals(command));

    command = client.configure(CConfiguration.create(), new String[]{"start", "--application", "appId",
      "--processor", "processor"});
    assertTrue("start".equals(command));

    command = client.configure(CConfiguration.create(), new String[]{"stop", "--application", "appId",
      "--processor", "processor"});
    assertTrue("stop".equals(command));

    command = client.configure(CConfiguration.create(), new String[]{"status", "--application", "appId",
      "--processor", "processor"});
    assertTrue("status".equals(command));

    command = client.configure(CConfiguration.create(), new String[]{"promote", "--vpc", "vpc_name",
      "--authtoken", "Auth token",
      "--application", "application"});
    assertTrue("promote".equals(command));

  }
}
