====================
Contributing to CDAP
====================

Here are instructions to get you started if you want to contribute to CDAP. 

Security Reports
================

Please *DO NOT* file an issue for security related problems. 
Please send your reports to `security@cask.co <mailto:security@cask.co>`__.

Creating Issues
===============

In order to file bugs or new feature requests, please use http://issues.cask.co.

Feature Requests
================

While proposing a new feature, we look for:

* An issue in http://issues.cask.co that describes the problem and the action that will be taken to fix it

  * Please review existing proposals to make sure the feature proposal doesn't already exist

* Email the `CDAP mailing list <mailto:cdap-dev@googlegroups.com>`__ 

  * Include "Feature: <Name of the feature>" as the subject of the email
  * Include a link to the issue

Build Environment
=================

For instructions on setting up your development environment, please see the
`BUILD.rst <https://github.com/caskco/cdap/blob/develop/BUILD.rst>`__ file.

Contribution Guidelines
=======================

Creating Issues
---------------
An issue should be created at http://issues.cask.co for any bugs or new features before anybody starts working on it. 
Please take a moment to check that an issue doesn't already exist before creating a new issue. 
If you see that a bug or an improvement exists already, add a "+1" to the issue to indicate that you have the same
problem as well. This will help us in prioritization.

Discuss on the Mailing List
---------------------------
We recommend that you discuss your plan on the mailing list 
`cdap-developers@cask.co <mailto:cdap-developers@cask.co>`__
before starting to code. This gives us a chance to add feedback and help point you in the right direction.

Pull Requests
-------------
We love having pull requests and we do our best to review them as quickly as possible. 
If your pull request is not accepted on the first try, don't be discouraged. 
If there is a problem with the implementation, you will receive feedback on what to improve.
Add a link to your PR on the issue, and in an email to `cdap-developers@cask.co <mailto:cdap-developers@cask.co>`__.

Conventions
-----------
* Fork the repository and make changes on your fork in a feature branch. The branch name should be 
  "XXXX-something", where "XXXX" is the number of the issue. 

* Submit the code, with unit tests for the changes and documentation, if applicable. Take a look at 
  the existing tests to get an idea of what's required. 
  Run the full test suite on your branch before submitting a pull request.
  This will check for common errors and style mistakes.

* Update the documentation while creating or changing new features. 
  Test your documentation for clarity and correctness.

* Write clean code. Uniformity in formatting code promotes ease of writing, reading, and maintenance. 

* Pull requests should have a clear description of the problem being addressed. 
  They must reference the issue(s) they address.

* Commit messages should include a short summary. 

* All pull requests will be reviewed by one or more committers. Discuss, then make the
  suggested modifications and push additional commits to your feature branch. Be
  sure to post a comment after pushing. The new commits will show up in the pull
  request automatically, but the reviewers will not be notified unless you comment. 

* Committers use LGTM ("Looks Good To Me") in comments on the code review to indicate acceptance. 
  The code will be merged after the reviewer comments a "LGTM" on the pull request.


License and Trademarks
======================

Copyright © 2015 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.
