package com.continuuity.performance.runner;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 *
 */
public class SimplePerfTest {
  private static int staticCounter = 0;
  private int counter = 0;

  @BeforeClass
  public static void myBeforeClassMethod() {
    System.out.println("I am in myBeforeClassMethod now.");
    System.out.println("I am about to leave myBeforeClassMethod.");
    System.out.println(++staticCounter);
  }

  @Before
  public void myBeforeMethod() {
    System.out.println("I am in myBeforeMethod now.");
    System.out.println("I am about to leave myBeforeMethod.");
    System.out.println(++counter);
  }

  @PerformanceTest
  public void myFirstTestMethod() {
    System.out.println("I am in myFirstTestMethod now.");
    System.out.println("I am about to leave myFirstTestMethod.");
    System.out.println(++counter);
  }

  @PerformanceTest
  public void mySecondTestMethod() {
    System.out.println("I am in mySecondTestMethod now.");
    System.out.println("I am about to leave mySecondTestMethod.");
    System.out.println(++counter);
  }


  @After
  public void myAfterMethod() {
    System.out.println("I am in myAfterMethod now.");
    System.out.println("I am about to leave myAfterMethod.");
    System.out.println(++counter);
  }

  @AfterClass
  public static void myAfterClassMethod() {
    System.out.println("I am in myAfterClassMethod now.");
    System.out.println("I am about to leave myAfterClassMethod.");
    System.out.println(++staticCounter);
  }
}
