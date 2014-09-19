# Contributing to CDAP

Here are instructions to get you started if you want to contribute to CDAP. 

## Security Reports

Please *DO NOT* file a issue for security related problems. Please send your reports to [security@cask.co](mailto:security@cask.co).

## Creating Issues
Inorder to file bugs or new feature requests please use [http://issues.cask.co](http://issues.cask.co) to file issues.

## Feature requests

While proposing a new feature, we look for:

* An issue in [http://issues.cask.co](http://issues.cask.co) that describes the problem and the action that will be taken to fix
  * Please review existing proposals to make sure the feature proposal doesn't already exists
* Email the [mailing list](mailto:cdap-developers@cask.co) 
  * Include "Feature: Name of the feature in the subject"
  * Link to the issue

## Build Environment
For instructions on setting up your development environment, please
see [BUILD.md](https://github.com/caskco/cdap/blob/develop/BUILD.md) file

## Contribution Guidelines

### Creating issues
An issue should be created at [http://issues.cask.co](http://issues.cask.co) for any bugs or new features before anybody starts working on it. Please take a moment to check that issue already doesn't exist before creating a new issue. If you see a bug or an improvement already add a "+1"to the issue to indicate the you have this problem as well. This will help us in prioritization

### Discussions on mailing lists
We recommend that you discuss your plan on the mailing list [cdap-developers@cask.co](mailto: cdap-developers@cask.co) before starting to code. This gives us a chance to give feedback and make sure to point you in the right direction.

### Pull requests
We love having pull requests and we do our best to review them as fast as possible. If your pull request is not accepted on the first try, don't be discouraged. If there is a problem with the implementation, you will receive feedback on what to improve.

### Conventions

Fork the repository and make changes on your fork in a feature branch. The branch name should be XXXX-something, where XXXX is the number of the issue. 

Submit the code, unit tests for the changes and documentation if applicable. Take a look at the existing tests to get an idea. Run the full test suite on your branch before submitting a pull request. 

Update the documentation while creating or changing new features. Test your documentation for clarity and correctness.

Write clean code. Uniformity in formatting code promotes ease of writing, reading and maintenance. 

Pull requests should have clear description of the problem being addressed. Must reference the issue(s) they address.

Commit messages should include a short summary. 

All pull requests will be reviewed by one or more comitters. Discuss, then make the
suggested modifications and push additional commits to your feature branch. Be
sure to post a comment after pushing. The new commits will show up in the pull
request automatically, but the reviewers will not be notified unless you
comment. 

Committers use LGTM (Looks Good To Me) in comments on the code review to indicate acceptance. The code will be merged after the reviewer comments LGTM on the pull request.



