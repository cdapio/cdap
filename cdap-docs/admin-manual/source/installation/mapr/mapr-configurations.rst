.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. |display-distribution| replace:: MapR

.. |hdfs-user| replace:: ``cdap``

.. include:: /../target/_includes/mapr-configuration.rst
    :end-before: #. Depending on your installation, you may need to set these properties:

#. CDAP requires a classpath with an additional entry |---| ``/opt/mapr/lib/*`` |---| 
   appended to the ``yarn.application.classpath`` setting of ``yarn-site.xml``. The
   default ``yarn.application.classpath`` for Linux with this additional entry appended is
   (reformatted to fit)::

     $HADOOP_CONF_DIR, 
     $HADOOP_COMMON_HOME/share/hadoop/common/*, 
     $HADOOP_COMMON_HOME/share/hadoop/common/lib/*, 
     $HADOOP_HDFS_HOME/share/hadoop/hdfs/*, 
     $HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*, 
     $HADOOP_YARN_HOME/share/hadoop/yarn/*, 
     $HADOOP_YARN_HOME/share/hadoop/yarn/lib/*, 
     $HADOOP_COMMON_HOME/share/hadoop/mapreduce/*, 
     $HADOOP_COMMON_HOME/share/hadoop/mapreduce/lib/*, 
     /opt/mapr/lib/*
    
   **Note:** Since MapR might not dereference the Hadoop variables (such as
   ``$HADOOP_CONF_DIR``) correctly, we recommend specifying their full paths instead.

   MapR does not, by default, provide a configured ``yarn.application.classpath``, and you
   will need to add this entry to ``yarn-site.xml``. If you install using `Chef
   <https://www.getchef.com>`__, that file and entry is created automatically, but not
   with dereferenced Hadoop variables.

#. Depending on your installation, you may need to set these properties:

.. include:: /../target/_includes/mapr-configuration.rst
    :start-after: #. Depending on your installation, you may need to set these properties:
    :end-before: .. _mapr-configuration-security:
    
As in all installations, the ``kafka.log.dir`` may need to be created locally. If you
configure ``kafka.log.dir`` (or any of the other settable parameters) to a particular
directory, you need to make sure that **the directory exists** and that it **is writable**
by the CDAP user.

.. _mapr-configuration-security:

.. include:: /../target/_includes/mapr-configuration.rst
    :start-after: .. _mapr-configuration-security:
    :end-before: .. _mapr-configuration-hdp:
