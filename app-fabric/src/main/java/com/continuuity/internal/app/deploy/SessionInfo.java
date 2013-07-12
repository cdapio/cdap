/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.app.services.DeployStatus;
import com.continuuity.app.services.ResourceIdentifier;
import com.continuuity.app.services.ResourceInfo;
import com.continuuity.weave.filesystem.Location;
import com.google.common.base.Objects;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

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
   * Size of the file.
   */
  private int size;

  /**
   * Location of the archive file.
   */
  private Location archive;

  /**
   * Redundant, but useful resource information.
   */
  private transient ResourceIdentifier identifier;

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
   *
   * @param info about the resource being uploaded.
   */
  public SessionInfo(ResourceIdentifier identifier, ResourceInfo info, Location archive, DeployStatus status) {
    this.identifier = identifier;
    this.regtime = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    this.filename = info.getFilename();
    this.size = info.getSize();
    this.archive = archive;
    this.status = status;
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
   * Returs the resource identifier associated with the resource.
   * @return resource identifier.
   */
  public ResourceIdentifier getResourceIdenitifier() {
    return identifier;
  }

  /**
   * Returns location of the resource.
   * @return Location to the resource.
   */
  public Location getArchiveLocation() {
    return archive;
  }

  /**
   * Returns the size of the resource being uploaded
   *
   * @return size of the resource being uploaded.
   */
  public int getFileSize() {
    return size;
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
      Objects.equal(size, that.size) &&
      Objects.equal(regtime, that.regtime) &&
      Objects.equal(archive, that.archive) &&
      Objects.equal(identifier, that.identifier);
  }

  /**
   * Returns the hashcode for this object.
   *
   * @return hash code for this object.
   */
  public int hashCode() {
    return Objects.hashCode(filename, size, regtime, archive, identifier);
  }

  /**
   * Returns string representation of this object.
   *
   * @return string representation of this object.
   */
  public String toString() {
    return Objects.toStringHelper(this)
      .add("filename", filename)
      .add("size", size)
      .add("regtime", regtime)
      .add("archive", archive)
      .add("identifier", identifier)
      .toString();
  }
}
