package functions

import java.text.SimpleDateFormat
import java.util.Date

import PB.Quote.Quote.Transaction
import data.TX
import org.apache.flink.api.common.functions.MapFunction


class TransactionMapFunction extends MapFunction[Transaction,TX]{

  override def map(t: Transaction): TX = {

    val format = new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSS")
    val d: Date = format.parse(t.getDate.toString + " 00:00:00.000")
    val time = d.getTime + t.getTime

    TX(t.getDate.toString,time,t.getCode,t.getNIndex,t.getLastprice,t.getVolume,t.getTurnover)
  }
}
