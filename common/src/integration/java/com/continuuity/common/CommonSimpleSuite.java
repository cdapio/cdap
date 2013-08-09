package com.continuuity.common;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;


/**
 * Test suite for common.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({OtherTest.class })
public class CommonSimpleSuite {
    static {
        System.out.println("suite class loaded");
    }

    @org.junit.BeforeClass public static void init() {
        System.out.println("before suite class out");
        System.out.println("non-asci char: ż");
        System.err.println("before suite class err");
    }
    @org.junit.AfterClass public static void end() {
        System.out.println("after suite class out");
        System.err.println("after suite class err");
    }
}
