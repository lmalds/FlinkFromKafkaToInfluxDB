
import java.util.Properties

import org.apache.flink.streaming.api.scala._
import PB.Quote.Quote.Transaction
import data.TransactionDeserializationSchema
import functions.TransactionMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import sinks.InfluxDBSink

object Job {

  def main(args : Array[String]) : Unit = {

    // set up the execution environment
    val env =StreamExecutionEnvironment.getExecutionEnvironment
    //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Kafka consumer properties
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "flink:9092,data0:9092,mf:9092")
    kafkaProps.setProperty("group.id", "lmalds-demo")

    val transactionStream = env.addSource[Transaction](new FlinkKafkaConsumer09[Transaction]("transaction",new TransactionDeserializationSchema,kafkaProps)).setParallelism(1)

    val txStream = transactionStream.map(new TransactionMapFunction).setParallelism(1)

    txStream.addSink(new InfluxDBSink("transaction")).setParallelism(1)

    // Write this transaction stream out to print
    txStream.print().setParallelism(1)

    // execute program
    env.execute("lmalds example")
  }

}
