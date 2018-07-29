.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2018 Cask Data, Inc.

:hide-toc: true

.. _cloud-runtimes-profiles:

========
Profiles
========

.. toctree::
    :maxdepth: 4

    Creating Profiles <creating-profiles>
    Assigning Profiles <assigning-profiles>
    Admin Controls <admin-controls>

Profiles consist of a provisioner name and provisioner configuration settings. They can be
created by CDAP admins and assigned to batch pipelines. When a pipeline is assigned to a pipeline,
CDAP will use the provisioner specified in the profile to create a cluster, run the pipeline on
that cluster, then tear down the cluster after the run has finished.

.. |creating-profiles| replace:: **Creating Profiles:**
.. _creating-profiles: creating-profiles.html

.. |assigning-profiles| replace:: **Assigning Profiles:**
.. _assigning-profiles: assigning-profiles.html

.. |admin-controls| replace:: **Admin Controls:**
.. _admin-controls: admin-controls.html

- |creating-profiles|_ How to create and configure profiles

- |assigning-profiles|_ How to assign profiles to pipelines

- |admin-controls|_ How to monitor and control profile usage, such as enabling and disabling profiles

