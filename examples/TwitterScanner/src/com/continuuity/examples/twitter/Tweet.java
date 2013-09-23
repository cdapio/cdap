/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.continuuity.examples.twitter;

import java.util.Date;

import twitter4j.Status;



/**
 * Tweet Placeholder class.
 */
public class Tweet {

  private Date date;
  private Long id;
  private String text;
  private String user;

  /**
   * Empty constructor for Kryo serialization.
   */
  public Tweet() {
  }

  /**
   * Constructs a tweet from the specified twitter4j Status object.
   *
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
