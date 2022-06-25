package com.wps.flink

import org.apache.flink.api.common.typeinfo.TypeInformation

class TXDeserialization() extends java.lang.Object with org.apache.flink.api.common.serialization.DeserializationSchema[scala.Array[scala.Byte]]{
  override def deserialize(bytes: Array[Byte]): Array[Byte] = ???

  override def isEndOfStream(t: Array[Byte]): Boolean = ???

  override def getProducedType: TypeInformation[Array[Byte]] = ???
}
