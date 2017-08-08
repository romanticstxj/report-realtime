import sbt._

object Dependencies {
  // Versions
  val sparkVersion = "2.2.0"
  val avroVersion  = "1.8.2"

  // Libraries
  val config     = "com.typesafe"       % "config"                    % "1.3.1"
  val kafka      = "org.apache.kafka"   % "kafka-clients"             % "0.10.1.1"
  val sparkSQL   = "org.apache.spark"  %% "spark-sql"                 % sparkVersion   % Provided
  val sparkSK    = "org.apache.spark"  %% "spark-sql-kafka-0-10"      % sparkVersion     exclude("org.apache.kafka", "kafka-clients")
  val avro       = "org.apache.avro"    % "avro"                      % avroVersion
  val hadoopC    = "org.apache.hadoop"  % "hadoop-client"             % "2.7.3"        % Provided
  val mysqlConn  = "mysql"              % "mysql-connector-java"      % "5.1.43"       % Provided
  
  // Projects
  val rootDeps = Seq(config, kafka, sparkSQL, sparkSK, hadoopC, avro, mysqlConn)

  // Resolvers
  val rootResolvers = Seq()
}