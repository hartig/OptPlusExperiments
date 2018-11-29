# OPT+ Experiments
This repo contains the source code of the tools that we have used for our OPT+ experiments.

## Setup
The tools can be built using Maven. However, before building, two Java libraries have to be installed manually because these two dependencies are not (yet) available on a Maven server.

The first of these dependencies is the latest version 2.1 of the [HDT Java library](https://github.com/rdfhdt/hdt-java). To install this library, first clone the corresponding github repo as follows.
```
git clone git@github.com:rdfhdt/hdt-java.git
```
Next, install the library using Maven:
```
cd hdt-java
mvn install
cd ..
```

The second dependency is our [OPT+ extension for Apache Jena](https://github.com/hartig/OptPlus4Jena). The github repo of this library also has to be cloned and installed using Maven:
```
git clone git@github.com:hartig/OptPlus4Jena.git
cd OptPlus4Jena
mvn install
cd ..
```

Now you can clone this repo with the experiment tools and build the tools using Maven:
```
git clone git@github.com:hartig/OptPlusExperiments.git
cd OptPlusExperiments
mvn package
```

## Usage
Start the experiment runs as follows:
```
java -Xms4G -Xmx4G -cp target/OptPlusExperiments-0.0.1-SNAPSHOT.jar se.liu.ida.jenaext.optplus.RunExperiment --queryids=stats.csv --querydir=queries --hdtfile dbpedia.3.5.1_merged.nt.hdt
```
