package com.continuuity.examples.twitter;

import java.util.Date;

import twitter4j.Status;

public class Tweet {

  private Date date;
  private Long id;
  private String text;
  private String user;
  
  /** Empty constructor for Kryo serialization */
  public Tweet() {}

  /**
   * Constructs a Tweet from the specified twitter4j Status object.
   * @param status
   */
  public Tweet(Status status) {
    this.date = status.getCreatedAt();
    this.id = status.getId();
    this.text = status.getText();
    this.user = status.getUser().getName();
  }

  public Date getDate() {
    return date;
  }

  public Long getId() {
    return id;
  }

  public String getText() {
    return text;
  }

  public String getUser() {
    return user;
  }

  @Override
  public String toString() {
    return "{@" + user + " : " + text + " @ " + date.toString() + "}";
  }
}
