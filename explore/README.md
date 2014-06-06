about hive-exec patch file
--------------------------

This patch file contains all the changes that had to be made to hive branch `release-0.13.0-rc2`
to make it work with reactor 2.3.0 in singlenode and in unit-tests.
A hive-exec-0.13.0-continuuity-2.3.0.jar is already uploaded to Nexus and is used by Reactor in
singlenode and for unit tests.

To modify the hive-exec jar again, follow these steps:

1. Clone Hive repository from Github and checkout to branch `release-0.13.0-rc2`
2. Create another branch: ``git checkout -b my_patched_branch``
3. Make your changes in the ``ql`` module (it will be packaged as hive-exec.jar)
4. You can modify ``pom.xml`` in module ``ql`` to change the version number of the generated jar, according
to what version of Reactor you are modifying.
5. Deploy your jar to Nexus using the following command in the ``ql`` directory:
  ``mvn clean deploy -DskipTests -P hadoop-2``
6. Change your dependencies in Reactor to point to the same version number you specified in step 4. for
hive-exec.
7. Don't forget to create a new patch file and to put it here:
  ``git format-patch release-0.13.0-rc2 --stdout > my_new_hive-exec-path.patch``

