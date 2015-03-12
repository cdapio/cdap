.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _application-logback:

=====================
Application Logbacks
=====================

YARN containers spawned by a CDAP application use the default container logback, `logback-container.xml`,
packaged with CDAP. This `logback.xml` does log rotation and depending on the use case, may be sufficient.
However, it is also possible to specify a `logback.xml` for a CDAP application.
The packaged `logback.xml` is then used for each container spawned by a program of the application.

Writing and packaging a logback.xml
===================================
To specify a `logback.xml` to be used by the application, it is sufficient to package the file as a resource in the jar.
The `pom.xml` of the application may look like this, with the `resources` directory containing the `logback.xml`::

  <?xml version="1.0" encoding="UTF-8"?>
    <project xmlns="http://maven.apache.org/POM/4.0.0"
             xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
             xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    ...

      <build>
        <resources>
          <resource>
            <directory>resources</directory>
          </resource>
        </resources>
      </build>

    ...

  </project>

CDAP supports standard logback functionality. To write a custom logback you may
refer to `LOGBack <http://logback.qos.ch/>`__. An important thing to note is that when a `logback.xml` is
specified for an application, CDAP does not handle log rotation. It is usually a good idea for your logback
to do rotation and expiry, to ensure that long running containers don't fill up the disk.


