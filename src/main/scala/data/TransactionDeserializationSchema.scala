package data

import PB.Quote.Quote.Transaction
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.util.serialization.DeserializationSchema


class TransactionDeserializationSchema extends DeserializationSchema[Transaction]{

  override def isEndOfStream(t: Transaction): Boolean = {false}

  override def deserialize(bytes: Array[Byte]): Transaction = {
    PB.Quote.Quote.Transaction.parseFrom(bytes)
  }

  override def getProducedType: TypeInformation[Transaction] = {
    TypeInformation.of(new TypeHint[Transaction](){})
  }
}
