# Cask Data Application Platform (CDAP) Spark Java Archetype

This directory contains Java Archetype for the Cask Data Application Platform (CDAP).

# Installing

To install archetype, enter from cdap root directory:

> mvn clean install -f archetypes/cdap-spark-java-archetype/pom.xml

# Creating

To create project from archetype, use following script as example:

```
 mvn archetype:generate 					
  -DarchetypeGroupId=co.cask.cdap 			
  -DarchetypeArtifactId=cdap-spark-java-archetype 	
  -DarchetypeVersion=2.5.0-SNAPSHOT 			
  -DgroupId=org.company 					
  -DartifactId=SparkPageRank 				
  -Dversion=1.0						

```  

## License and Trademarks

Copyright © 2014 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the 
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions 
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.