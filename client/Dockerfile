FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
COPY src ./src
RUN mvn -q package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/target/bbs-bot.jar /app/bbs-bot.jar
CMD ["java", "-jar", "/app/bbs-bot.jar"]
