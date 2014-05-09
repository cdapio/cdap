/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.app.services.DeployStatus;
import com.google.common.base.Objects;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Session information associated with uploading of an archive.
 */
public final class SessionInfo {

  /**
   * Time of registration.
   */
  private long regtime;

  /**
   * Filename that is being uploaded.
   */
  private String filename;

  /**
   * Location of the archive file.
   */
  private Location archive;

  /**
   * Application id. {@code null} means use app name provided by app spec
   */
  @Nullable
  private String applicationId;

  private String accountId;

  /**
   * Outputstream associated with file.
   */
  private transient OutputStream stream = null;

  /**
   * Status of deployment.
   */
  private DeployStatus status;

  /**
   * No-Op constructor.
   */
  public SessionInfo() {}

  /**
   * Constructs the object with identifier and resource info provided.
   */
  public SessionInfo(String accountId, String appId, String filename, Location archive, DeployStatus status) {
    this.accountId = accountId;
    this.regtime = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    this.filename = filename;
    this.archive = archive;
    this.status = status;
    this.applicationId = appId;
  }

  /**
   * Returns registration time of the resource.
   *
   * @return registration time the resource entered the system.
   */
  public long getRegistrationTime() {
    return regtime;
  }

  /**
   * Sets a registeration time.
   * @param registrationTime time the resource was registered in the system.
   */
  public SessionInfo setRegistrationTime(long registrationTime) {
    this.regtime = registrationTime;
    return this;
  }

  /**
   * Returns name of the file being uploaded.
   *
   * @return name of the file being uploaded.
   */
  public String getFilename() {
    return filename;
  }


  /**
   * Returns location of the resource.
   * @return Location to the resource.
   */
  public Location getArchiveLocation() {
    return archive;
  }

  /**
   * @return application id, {@code null} means use app name provided by app spec
   */
  @Nullable
  public String getApplicationId() {
    return applicationId;
  }

  public DeployStatus getStatus() {
    return status;
  }

  public SessionInfo setStatus(DeployStatus status) {
    this.status = status;
    return this;
  }

  /**
   * Why synchronized ? Add comment.
   * @return
   * @throws IOException
   */
  public synchronized OutputStream getOutputStream() throws IOException {
    if (stream == null) {
      stream = archive.getOutputStream();
    }
    return stream;
  }

  /**
   * Compares this object with other object for equality.
   *
   * @param other object with which this object is compared with.
   * @return true if same; false otherwise.
   */
  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    SessionInfo that = (SessionInfo) other;
    return
      Objects.equal(filename, that.filename) &&
      Objects.equal(regtime, that.regtime) &&
      Objects.equal(archive, that.archive) &&
      Objects.equal(accountId, that.accountId) &&
      Objects.equal(applicationId, that.applicationId);
  }

  /**
   * Returns the hashcode for this object.
   *
   * @return hash code for this object.
   */
  public int hashCode() {
    return Objects.hashCode(filename, regtime, archive, accountId, applicationId);
  }

  /**
   * Returns string representation of this object.
   *
   * @return string representation of this object.
   */
  public String toString() {
    return Objects.toStringHelper(this)
      .add("filename", filename)
      .add("regtime", regtime)
      .add("archive", archive)
      .add("accountId", accountId)
      .add("appId", applicationId)
      .toString();
  }
}
