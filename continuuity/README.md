Continuuity Master Project
==========================

This is the master continuuity root repository, which is located at github.com/continuuity/continuuity.git.   
To build the master project simply clone the master repo and run "gradle".  This command will clone all the 
required project repos and run a build across all projects.

Git Tasks
---------

Because the master git repo doesn't actually contain the individual projects, it needs to pull them from git 
when performing a build. A number of git related tasks are included in the master build.gradle for helping to 
work across all the different repos.

- __gitClone__      Clones all the repositories listed in the master build file. This task does nothing if the repo directory already exists.
- __gitCheckout__   Checkouts the branch listed in the master build file.
- __gitPull__       Pulls the latest updates for each of the repositories.
- __gitUpdate__     Runs clone, checkout, and pull for each repository. This task is used by the CI server to ensure the build source is up-to-date.

For more information you should read this wiki page: https://wiki.continuuity.com/display/ENG/Continuuity+Build+System
