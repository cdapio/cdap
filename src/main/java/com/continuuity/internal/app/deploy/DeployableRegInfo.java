package com.continuuity.internal.app.deploy;

import com.continuuity.app.services.ResourceIdentifier;
import com.continuuity.app.services.ResourceInfo;
import com.continuuity.filesystem.Location;
import com.google.common.base.Objects;

/**
 * Registration data that is stored in Zookeeper under a resource id.
 */
public final class DeployableRegInfo {

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
   * Location of the jar file.
   */
  private Location jar;

  /**
   * Redundant, but useful resource information.
   */
  private ResourceIdentifier identifier;

  /**
   * No-Op constructor.
   */
  public DeployableRegInfo() {}

  /**
   * Constructs the object with identifier and resource info provided.
   *
   * @param info about the resource being uploaded.
   */
  public DeployableRegInfo(ResourceIdentifier identifier, ResourceInfo info, Location jar) {
    this.identifier = identifier;
    this.regtime = System.currentTimeMillis()/1000;
    this.filename = info.getFilename();
    this.size = info.getSize();
    this.jar = jar;
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
  public void setRegistrationTime(long registrationTime) {
    this.regtime = registrationTime;
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
  public Location getJarLocation() {
    return jar;
  }

  /**
   * Returns the size of the resource being uploaded
   *
   * @return size of the resource being uploaded.
   */
  public int getFileSize() {
    return size;
  }

  /**
   * Compares this object with other object for equality.
   *
   * @param other object with which this object is compared with.
   * @return true if same; false otherwise.
   */
  @Override
  public boolean equals(Object other) {
    if(other == null) {
      return false;
    }
    DeployableRegInfo that = (DeployableRegInfo) other;
    return
      Objects.equal(filename, that.filename) &&
      Objects.equal(size, that.size) &&
      Objects.equal(regtime, that.regtime) &&
      Objects.equal(jar, that.jar) &&
      Objects.equal(identifier, that.identifier);
  }

  /**
   * Returns the hashcode for this object.
   *
   * @return hash code for this object.
   */
  public int hashCode() {
    return Objects.hashCode(filename, size, regtime, jar, identifier);
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
      .add("jar", jar)
      .add("identifier", identifier)
      .toString();
  }
}
