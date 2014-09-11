# Cask Data Application Platform (CDAP) Spark Scala Archetype

This directory contains Scala Archetype for the Cask Data Application Platform (CDAP).

# Installing

To install archetype, enter from cdap root directory:

> mvn clean install -f archetypes/cdap-spark-scala-archetype/pom.xml

# Creating

To create project from archetype, use following script as example:

```
 mvn archetype:generate 					
  -DarchetypeGroupId=co.cask.cdap 			
  -DarchetypeArtifactId=cdap-spark-scala-archetype 	
  -DarchetypeVersion=2.5.0-SNAPSHOT 			
  -DgroupId=org.company 					
  -DartifactId=SparkKMeans 				
  -Dversion=1.0						

```  


