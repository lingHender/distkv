FROM openjdk:8-jdk-alpine
VOLUME /tmp
EXPOSE 8080
ARG JAR_FILE=target/DistributedDB-1.0-SNAPSHOT.jar
COPY ${JAR_FILE} DistributedDB.jar
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/DistributedDB.jar"]
