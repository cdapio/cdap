Continuuity Reactor (TM)
========================
Local, Sandbox and Enterprise Continuuity Reactors

Build Reactor with Maven
---------------------------

### Clean all modules
    mvn clean

### Run all tests, fail at the end
    mvn test -fae
    
### Build all modules
    mvn package

### Run checkstyle, skipping tests
    mvn package -DskipTests

### Build a particular module
    mvn package -pl [module] -am

### Run selected test
    mvn -Dtest=TestClass,TestMore*Class,TestClassMethod#methodName -DfailIfNoTests=false test

See [Surefire doc](http://maven.apache.org/surefire/maven-surefire-plugin/examples/single-test.html) for more details

### Build all examples
    mvn package -DskipTests -pl examples -am -amd -P examples

### Build Singlenode distribution ZIP
    mvn clean package -DskipTests -P examples && mvn package -pl singlenode -am -DskipTests -P dist,release

### Build distributions (rpm, deb, tgz)
    mvn package -DskipTests -P dist,rpm-prepare,rpm,deb-prepare,deb,tgz

### Show dependency tree
    mvn package dependency:tree -DskipTests

### Show dependency tree for a particular module
    mvn package dependency:tree -DskipTests -pl [module] -am

### Show test output to stdout
    mvn -Dsurefire.redirectTestOutputToFile=false ...

### Offline mode
    mvn -o ....

### Change version
    mvn versions:set -DnewVersion=[new_version] -DgenerateBackupPoms=false
