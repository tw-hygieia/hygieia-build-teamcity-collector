---
title: Teamcity Build Collector
tags:
keywords:
summary:
sidebar: hygieia_sidebar
permalink: teamcity.html
---
Configure the Teamcity Collector to display and monitor information (related to build status) on the Hygieia Dashboard, from Teamcity. Hygieia uses Spring Boot to package the collector as an executable JAR file with dependencies.

### Setup Instructions

## Fork and Clone the Collector 

Fork and clone the Teamcity Collector from the [GitHub repo](https://github.com/tw-hygieia/hygieia-build-teamcity-collector). 

To configure the Teamcity Collector, execute the following steps:


*   **Step 1: Change Directory**

Change the current working directory to the `hygieia-build-teamcity-collector` directory of your Hygieia source code installation.

For example, in the Windows command prompt, run the following command:

```
cd C:\Users\[username]\hygieia-build-teamcity-collector
```

*   **Step 2: Run Maven Build**

Run the maven build to package the collector into an executable JAR file:

``` 
mvn install
```

The output file `[collector name].jar` is generated in the `hygieia-build-teamcity-collector\target` folder.

*   **Step 3: Set Parameters in Application Properties File**

Set the configurable parameters in the `application.properties` file to connect to the Dashboard MongoDB database instance, including properties required by the Teamcity Collector.

To configure parameters for the Teamcity Collector, refer to the sample [application.properties](#sample-application-properties-file) file.

For information about sourcing the application properties file, refer to the [Spring Boot Documentation](http://docs.spring.io/spring-boot/docs/current-SNAPSHOT/reference/htmlsingle/#boot-features-external-config-application-property-files).

*   **Step 4: Deploy the Executable File**

To deploy the `[collector name].jar` file, change directory to `hygieia-build-teamcity-collector\target`, and then execute the following from the command prompt:

```
java -jar [collector name].jar --spring.config.name=teamcity --spring.config.location=[path to application.properties file]
```

### Sample Application Properties File

```properties
# Database Name
dbname=dashboarddb

# Database HostName - default is localhost
dbhost=localhost

# Database Port - default is 27017
dbport=9999

# MongoDB replicaset
dbreplicaset=[false if you are not using MongoDB replicaset]
dbhostport=[host1:port1,host2:port2,host3:port3]

# Database Username - default is blank
dbusername=dashboarduser

# Database Password - default is blank
dbpassword=dbpassword

# Collector schedule (required)
teamcity.cron=0 0/5 * * * *

# The page size
teamcity.projectIds=testproject1,testproject2

# The folder depth - default is 10
teamcity.folderDepth=10

# Teamcity server (required) - Can provide multiple
teamcity.servers[0]=http://teamcity.company.com

# If using username/token for API authentication
# (required for Cloudbees Teamcity Ops Center) For example,
teamcity.servers[1]=http://username:token@teamcity.company.com

# Another option: If using same username/password Teamcity auth,
# set username/apiKey to use HTTP Basic Auth (blank=no auth)
teamcity.usernames[0]=
teamcity.apiKeys[0]=

# Determines if build console log is collected - defaults to false
teamcity.saveLog=true
		
# Search criteria enabled via properties (max search criteria = 2) 
teamcity.searchFields[0]= options.jobName
teamcity.searchFields[1]= niceName 

# Timeout values
teamcity.connectTimeout=20000
teamcity.readTimeout=20000
```
