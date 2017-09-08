package com.madhouse.ssp.util

import com.madhouse.ssp.avro._
import com.madhouse.ssp.entity.LogType._
import org.apache.avro.Schema
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.{SpecificDatumReader, SpecificRecordBase}

/**
  * Created by Sunxiang on 2017-07-28 09:50.
  *
  */
object AvroUtil extends Serializable {

  private def recordDecode[T <: SpecificRecordBase](bytes: Array[Byte], schema: Schema): T = {
    val reader = new SpecificDatumReader[T](schema)
    val decoder = DecoderFactory.get.binaryDecoder(bytes, null)
    try {
      reader.read(null.asInstanceOf[T], decoder)
    } catch {
      case _: Exception => null.asInstanceOf[T]
    }
  }

  def recordDecode[T <: SpecificRecordBase](bytes: Array[Byte], logType: LogType): T = {
    val record = logType match {
      case MEDIABID => recordDecode[MediaBid](bytes, MediaBid.SCHEMA$)
      case DSPBID => recordDecode[DSPBid](bytes, DSPBid.SCHEMA$)
      case IMPRESSION => recordDecode[ImpressionTrack](bytes, ImpressionTrack.SCHEMA$)
      case CLICK => recordDecode[ClickTrack](bytes, ClickTrack.SCHEMA$)
      case _ => null
    }
    record.asInstanceOf[T]
  }

}