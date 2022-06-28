# Spark Inception Controller
## Data+AI Summit 2022: [Spark Inception: Exploiting the Spark REPL to Build Streaming Notebooks](https://databricks.com/dataaisummit/session/spark-inception-exploiting-apache-spark-repl-build-streaming-notebooks)

This container is the end result of building the source code located at https://github.com/newfront/spark-inception.

## Running the Demo
In order to run the demo, you need to have docker installed and create a bridged network `docker network create mde`.

1. Use the following `docker-compose.yml` to get `redis` up and running.
~~~
version: '3'

services:
  redis:
    image: redis:6.2
    container_name: redis
    hostname: redis
    networks:
      - mde
    ports:
      - 6379:6379
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 30s
      retries: 50
    restart: always

networks:
  mde:
    external: true
    name: mde
~~~

via `docker compose -f /path/to/docker-compose.yml up -d`

2. Once redis is up and running. It is time to spin up the spark-inception-controller.
~~~
docker run \
  -p 4040:4040 \
  --hostname spark-inception-controller \
  --network mde \
  -it newfrontdocker/spark-inception-controller:1.0.0 \
  bash
~~~

The spark-inception-controller container has Apache Spark located at `/opt/spark` and the `spark-inception-controller` is located in `/opt/spark/app`. Next, you'll spin up the Spark Structured Streaming application.

3. Start up the Spark Inception Controller Application
   From within the docker shell from `step 2`, execute the following command.

~~~
$SPARK_HOME/bin/spark-submit \
  --verbose \
  --master "local[*]" \
  --class "com.coffeeco.data.SparkInceptionControllerApp" \
  --deploy-mode "client" \
  --jars "/opt/spark/app/jars/spark-inception-controller.jar" \
  --conf "spark.driver.extraClassPath=/opt/spark/app/jars/spark-inception-controller.jar" \
  --driver-java-options "-Dconfig.file=/opt/spark/app/conf/application-live.conf" \
  /opt/spark/app/jars/spark-inception-controller.jar
~~~

Now you have a fully functioning Apache Spark application. The trouble is the application doesn't do anything since it is essentially a blank canvas waiting for your input. Consider this no different than a Notebook environment, you need to `add some content` in order for something amazing to happen. So we will do just that.

## Interacting with the Spark Inception Controller
We have two services running. The first is `redis`. Which introduced the Stream data type in Redis 5.0, which we will use as a lightweight streaming message bus, and the second service is our Spark Structured Streaming Application (which is configured to process up to 10 "commands" per micro-batch). Each "command" is the equivalent of a paragraph within a notebook (think Apache Zeppelin, Jupyter, or Databricks Notebooks).

The command RPC structure is as follows:
~~~
NetworkCommand {
  notebookId: String
  paragraphId: String
  command: String
  requestId: String
  userId: Option[String]
}
~~~

The **NetworkCommand** is used to send either `%spark` or `%sql` commands to be processed in the Spark Inception Controller.

Let's look at an example putting all the pieces together.

We will have **three `docker exec processes`** at play to trigger remote commands (redis#1) on our Spark Inception Controller (spark-inception-controller), and to receive data back (redis#2). The results of processing our NotebookCommand:command, aka a paragraph, are fed into a Redis HashSet (used in Spark as a memory optimized columnar table) and the Status of the command "Success, Failure, or Error" is tracked in the **NotebookExecutionDetails** object.

~~~
NotebookExecutionDetails {
  notebookId: String,
  paragraphId: String,
  command: String,
  requestId: String,
  userId: Option[String],
  commandStatus: String,
  result: String
}
~~~
This gives us all the pieces now to have fun and exploit Apache Spark.

1. Open up a new Terminal (Console) tab for our Redis Command Line
~~~
docker exec -it redis redis-cli
~~~

2. In another terminal window, we will monitor the commands and data being passed back and forth. This is our immediate feedback that a command was received, processed, and we can see what the result of running the command actually was!
~~~
docker exec -it redis redis-cli monitor
~~~

Now you can execute the following command: (**NetworkCommand**) in the terminal window you opened up in Step 1.

The following command creates a new case class **People**, generates a sequence of **People**, uses the SparkSession reference inside of the SparkILoop to generate a new DataFrame (on-the-fly), and then registers the data as a temp view named **people**.
~~~
xadd com:coffeeco:notebooks:v1:notebook1:rpc MAXLEN ~ 3000 * notebookId notebook1 paragraphId paragraph1 command "\n%spark\ncase class Person(name: String, age: Int)\nval people = Seq(Person(\"scott\",37),Person(\"willow\",12),Person(\"clover\",6))\nval df = spark.createDataFrame(people)\ndf.createOrReplaceTempView(\"people\")\n" requestId request1 userId "1000"
~~~

You will see lots of things happening in the process running the Spark Inception Controller. If this is the first command you've sent to the application, then the singleton `SparkILoop` will be scaffolded reusing the same `SparkSession` as our structured streaming application, and otherwise you will see the input/output information for the application. The **SparkILoop** is generated inside of the **SparkRemoteSession** class, and is essentially a customized instance of the `spark-shell` running inside of our Structured Streaming Application. This enables us to **define new classes, construct tables, you name it** all while never having thought of it ahead of time.

Let's view the contents of the table **people** we created using the first remote command. Pop back into the **redis-cli** (redis#1) from before, and execute the following command.
~~~
xadd com:coffeeco:notebooks:v1:notebook1:rpc MAXLEN ~ 3000 * notebookId notebook1 paragraphId paragraph2 command "\n%sql\nselect * from people" requestId request2 userId "1000"
~~~

You should now see the results of the people table output as newline separated JSON in redis.

~~~
hget "com:coffeeco:notebooks:v1:notebook1:results:paragraph2" result
~~~

Which will give you the table `(df.toJSON.collect.mkString("\n"))` of the **people** view you created with the first command.

~~~
"{\"name\":\"scott\",\"age\":37}\n{\"name\":\"willow\",\"age\":12}\n{\"name\":\"clover\",\"age\":6}"
~~~

That's it. You now have a Streaming Notebook environment using Spark to power the dynamic generation of new Spark applications, or just to fiddle around with and come up with whacky new ideas! 

## Making things Simpler
You can start the entire Spark Inception Controller using a single command. Now you have less the think about if you want to bother with other things. Just make sure you have started up `redis` as well - otherwise you'll get an error trying to start the application.
~~~
docker run \
  -p 4040:4040 \
  --hostname spark-inception-controller \
  --network mde \
  -it newfrontdocker/spark-inception-controller:1.0.0 \
  /opt/spark/bin/spark-submit \
  --verbose \
  --master "local[*]" \
  --class "com.coffeeco.data.SparkInceptionControllerApp" \
  --deploy-mode "client" \
  --jars "/opt/spark/app/jars/spark-inception-controller.jar" \
  --conf "spark.driver.extraClassPath=/opt/spark/app/jars/spark-inception-controller.jar" \
  --driver-java-options "-Dconfig.file=/opt/spark/app/conf/application-live.conf" \
  /opt/spark/app/jars/spark-inception-controller.jar
~~~
