package com.continuuity.cperf.runner;

//@Retention(RetentionPolicy.RUNTIME)
//@Target(ElementType.TYPE)
//@Inherited
public @interface Report {
    /**
     * @return a Runner class (must have a constructor that takes a single Class to run)
     */
    String[] value();
}