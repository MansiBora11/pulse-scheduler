FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
COPY pulse-server pulse-server
COPY pulse-client pulse-client
RUN mvn -B -q -pl pulse-server -am package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/pulse-server/target/pulse-server-*-SNAPSHOT.jar app.jar
COPY --from=build /src/pulse-server/target/lib lib
EXPOSE 8080
ENTRYPOINT ["java", "-XX:+UseZGC", "-jar", "app.jar"]
