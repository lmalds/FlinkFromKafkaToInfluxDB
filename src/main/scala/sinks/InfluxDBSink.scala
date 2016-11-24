package sinks

import java.util.concurrent.TimeUnit

import data.TX
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.influxdb.{InfluxDB, InfluxDBFactory}
import org.influxdb.dto.Point


class InfluxDBSink(measurement : String) extends RichSinkFunction[TX]{

  private val dataBaseName = "influxDemo"
  var influxDB : InfluxDB = null

  override def open(parameters : Configuration) : Unit = {
    super.open(parameters)
    influxDB = InfluxDBFactory.connect("http://192.168.4.41:8086", "admin", "admin")
    influxDB.createDatabase(dataBaseName)
    influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS)
  }

  override def close() : Unit = {
    super.close()
  }

  override def invoke(in: TX): Unit = {
    val builder = Point.measurement(measurement)
      .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
      .tag("code", in.code)
      .addField("nIndex", in.nindex)
      .addField("price", in.price)
      .addField("volume", in.volume)
      .addField("value", in.value)

    val p = builder.build()

    influxDB.write(dataBaseName,"autogen",p)
  }


}
