package com.continuuity.common.options;

import com.continuuity.common.utils.OSDetector;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

/**
 *
 */
public class OptionsParserTest {

  private static class MyFlags {
    @Option(usage = "a boolean flag") private boolean flagNoValue;
    @Option protected boolean flagOneDashBoolean;
    @Option(name = "flagFloat") public float flagNumber;
    @Option public double flagDouble;
    @Option public int flagInt;
    @Option public long flagLong;
    @Option public short flagShort;
    @Option public String flagString;
    @Option public String flagDefault = "defaultValue";

    // These two parameters may be controlled by explicit flags or by the environment.
    @Option(name = "home", envVar = "HOME", hidden = true) public String homeVar = "somedefault";
    @Option(name = "missing", envVar = "MISSING_ENV_VAR_XXXXXXX", hidden = true)
    public String missingEnv = "missing";

    private String notAFlag;

    public boolean getFlagNoValue() {
      return flagNoValue;
    }

    public boolean getFlagOneDashBoolean() {
      return flagOneDashBoolean;
    }

    public float getFlagFloat() {
      return flagNumber;
    }
  }

  private static class MySubclassedFlags extends MyFlags {
    @Option public String flagSubclass;
  }

  private static class UnsupportedTypeFlags {
    @Option private Object unsupportedFlagType;
  }

  private static class DuplicateFlagDeclaration {
    @Option private int myFlag;
    @Option(name = "myFlag") private String myDuplicateFlag;
  }

  private static class HelpOverride {
    @Option public boolean help;
    @Option public String foo;
  }

  @Test
  public void testParse() {
    MyFlags myFlags = new MyFlags();
    String[] args = new String[] {
      "nonFlagArg1",
      "--flagNoValue",
      "-flagOneDashBoolean=false",
      "nonFlagArg2",
      "--flagFloat=-10.234",
      "--flagDouble=0.1",
      "--flagInt=-3",
      "--flagShort=10",
      "--flagLong=123",
      "--flagString=foo",
    };

    List<String> nonFlagArgs = OptionsParser.init(myFlags, args, "OptionsParserTest", "0.1.0", System.out);

    Assert.assertTrue(myFlags.getFlagNoValue());
    Assert.assertFalse(myFlags.getFlagOneDashBoolean());
    Assert.assertEquals(-10.234f, myFlags.getFlagFloat(), 0.0001f);
    Assert.assertEquals(0.1, myFlags.flagDouble, 0.0001f);
    Assert.assertEquals(-3, myFlags.flagInt);
    Assert.assertEquals(10, myFlags.flagShort);
    Assert.assertEquals(123, myFlags.flagLong);
    Assert.assertTrue(myFlags.flagString.equals("foo"));
    Assert.assertTrue(myFlags.flagDefault.equals("defaultValue"));

    Assert.assertEquals(2, nonFlagArgs.size());
    Assert.assertTrue(nonFlagArgs.get(0).equals("nonFlagArg1"));
    Assert.assertTrue(nonFlagArgs.get(1).equals("nonFlagArg2"));
  }

  @Test
  public void testUnrecognizedFlag() {
    MyFlags myFlags = new MyFlags();
    String[] args = new String[] {
      "nonFlagArg1",
      "--flagNoValue",
      "-flagOneDashBoolean=false",
      "nonFlagArg2",
      "--flagFloat=-10.234",
      "--notAFlag=foo",
    };
    try {
      OptionsParser.init(myFlags, args, "OptionsParserTest", "0.1.0", System.out);
      Assert.assertTrue(false);  // Should have thrown an exception.
    } catch (UnrecognizedOptionException e) {
      Assert.assertTrue(e.getMessage().contains("notAFlag"));
    }
  }

  @Test
  public void testUnsupportedFlagType() {
    UnsupportedTypeFlags myFlags = new UnsupportedTypeFlags();
    String[] args = new String[] {
      "--unsupportedFlagType=null",
    };
    try {
      OptionsParser.init(myFlags, args, "OptionsParserTest", "0.1.0", System.out);
      Assert.assertTrue(false);  // Should have thrown an exception.
    } catch (UnsupportedOptionTypeException e) {
      Assert.assertTrue(e.getMessage().contains("unsupportedFlagType"));;
    }
  }

  @Test
  public void testIllegalFlagValue() {
    MyFlags myFlags = new MyFlags();
    String[] args = new String[] {
      "--flagFloat=notANumber",
    };
    try {
      OptionsParser.init(myFlags, args, "OptionsParserTest", "0.1.0", System.out);
      Assert.assertTrue(false);  // Should have thrown an exception.
    } catch (IllegalOptionValueException e) {
      Assert.assertTrue(e.getMessage().contains("flagFloat"));
    }
  }

  @Test
  public void testHexInt() {
    MyFlags myFlags = new MyFlags();
    String[] args = new String[] {
      "--flagInt=0xA",
    };
    try {
      OptionsParser.init(myFlags, args, "OptionsParserTest", "0.1.0", System.out);
      Assert.assertTrue(false);  // Should have thrown an exception.
    } catch (IllegalOptionValueException e) {
      // Parsing hex is not supported.
      Assert.assertTrue(e.getMessage().contains("flagInt"));
    }
  }

  @Test
  public void testNoEqualsSeparator() {
    MyFlags myFlags = new MyFlags();
    String[] args = new String[] {
      "--flagInt",
      "5",
    };
    try {
      OptionsParser.init(myFlags, args, "OptionsParserTest", "0.1.0", System.out);
      Assert.assertTrue(false);  // Should have thrown an exception.
    } catch (IllegalOptionValueException e) {
      // This will be treated as a flag without a value.
      Assert.assertTrue(e.getMessage().contains("flagInt"));
    }
  }

  @Test
  public void testDuplicateFlags() {
    DuplicateFlagDeclaration myFlags = new DuplicateFlagDeclaration();
    try {
      OptionsParser.init(myFlags, new String[] {}, "OptionsParserTest", "0.1.0", System.out);
      Assert.assertTrue(false);  // Should have thrown an exception.
    } catch (DuplicateOptionException e) {
      Assert.assertTrue(e.getMessage().contains("myFlag"));
    }
  }

  @Test
  public void testHelpOverride() {
    HelpOverride myFlags = new HelpOverride();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    OptionsParser.init(myFlags, new String[] {"--help", "--foo=bar"},
                       "OptionsParserTest", "0.1.0", new PrintStream(out));

    // No usage info should have printed, since we declared our own help flag.
    Assert.assertTrue(out.toString().isEmpty());

    Assert.assertTrue(myFlags.help);
    Assert.assertTrue(myFlags.foo.equals("bar"));
  }

  @Test
  public void testIgnoreAfterDoubleDashMarker() {
    MyFlags myFlags = new MyFlags();
    OptionsParser.init(myFlags, new String[] {"--flagInt=7", "--", "--flagDefault=foo"},
                       "OptionsParserTest", "0.1.0", System.out);

    Assert.assertEquals(7, myFlags.flagInt);
    // flagDefault should not have changed, since it was after the "--".
    Assert.assertEquals("defaultValue" , myFlags.flagDefault);
  }

  @Test
  public void testKeepLatestFlag() {
    MyFlags myFlags = new MyFlags();
    OptionsParser.init(myFlags, new String[] {"--flagInt=7", "--flagInt=8"}, "OptionsParserTest", "0.1.0", System.out);

    // Keeps the last flag value.
    Assert.assertEquals(8, myFlags.flagInt);
  }

  @Test
  public void testSubclassedFlags() {
    // Make sure the subclass inherits its superclass's flags.
    MySubclassedFlags myFlags = new MySubclassedFlags();
    Assert.assertNotNull(OptionsParser.init(myFlags,
      new String[] {"--flagInt=7", "--flagSubclass=foo"}, "OptionsParserTest", "0.1.0", System.out));

    Assert.assertEquals(7, myFlags.flagInt);
    Assert.assertEquals("foo", myFlags.flagSubclass);
  }

  @Test
  public void testEnvVarFlags() {
    // Ignore this test if windows as this does not work
    if (OSDetector.isWindows()) {
      return;
    }
    MyFlags myFlags = new MyFlags();
    OptionsParser.init(myFlags, new String[] { "--flagInt=7" }, "OptionsParserTest", "0.1.0", System.out);
    Assert.assertTrue(myFlags.homeVar.startsWith("/")); // should be some path.

    myFlags = new MyFlags();
    OptionsParser.init(myFlags, new String[] { "--home=meep" }, "OptionsParserTest", "0.1.0", System.out);
    Assert.assertEquals("meep", myFlags.homeVar);

    myFlags = new MyFlags();
    OptionsParser.init(myFlags, new String[] { "--missing=wombat" }, "OptionsParserTest", "0.1.0", System.out);
    Assert.assertEquals("wombat", myFlags.missingEnv);

    myFlags = new MyFlags();
    OptionsParser.init(myFlags, new String[] { }, "OptionsParserTest", "0.1.0", System.out);
    Assert.assertEquals("missing", myFlags.missingEnv);
  }
}
