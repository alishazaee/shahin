FROM 172.19.10.15:5020/library/gradle:8.14.3-jdk17

WORKDIR /app

COPY build.gradle settings.gradle /app/

COPY gradle/wrapper /app/gradle/wrapper

RUN gradle dependencies --no-daemon

COPY . /app

EXPOSE 8082

RUN gradle :consumer:clean :consumer:shadowJar --no-daemon

ARG JAR_FILE=consumer/build/libs/consumer-all.jar

RUN cp $JAR_FILE app.jar

ENTRYPOINT ["java", "-jar", "app.jar"]