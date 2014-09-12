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

