ARG app_jar=./target/scala-2.12/spark-inception-controller-assembly-1.0.0-SNAPSHOT.jar
ARG spark_user=1000
ARG spark_base_version=apache-spark-base:spark-3.2.1-jre-11-scala-2.12

FROM newfrontdocker/${spark_base_version}
ARG app_jar
ARG spark_user

USER ${spark_user}


# copy and rename
COPY --chown=${spark_user} ${app_jar} /opt/spark/app/jars/spark-inception-controller.jar

# copy the config for the app
COPY --chown=${spark_user} conf /opt/spark/app/conf
COPY --chown=${spark_user} repl /opt/spark/app/repl
COPY --chown=${spark_user} user_jars /opt/spark/app/user_jars

EXPOSE 4040