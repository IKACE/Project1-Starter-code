# Project1-Part1-Starter-code

Each directory is a subproject which can be built dependently.
Build the subproject:
> ./gradlew clean jar

This will produce a jar file with all dependencies and source code in the jar. The jar file can be found in the directory: build/libs/
To run the project on Great Lakes, please use the command:
> hadoop jar build/libs/<jarname>.jar <with any additional arguments included here>

Before submitting to autograder, please run
> ./gradlew clean jar

for each subproject.
