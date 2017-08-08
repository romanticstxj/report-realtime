package com.madhouse.ssp.util

import java.io.ByteArrayOutputStream

import com.madhouse.ssp.Configure
import com.madhouse.ssp.avro._
import org.apache.avro.Schema
import org.apache.avro.io.{BinaryEncoder, DecoderFactory, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter, SpecificRecordBase}

/**
  * Created by Sunxiang on 2017-07-28 09:50.
  *
  */
object AvroUtil {

  def recordEncode[T <: SpecificRecordBase](record: T) = {
    val writer = new SpecificDatumWriter[T](record.getSchema)
    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.binaryEncoder(out, null.asInstanceOf[BinaryEncoder])
    writer.write(record, encoder)
    encoder.flush()
    out.close()
    out.toByteArray
  }

  def recordDecode[T <: SpecificRecordBase](bytes: Array[Byte], schema: Schema): T = {
    val reader = new SpecificDatumReader[T](schema)
    val decoder = DecoderFactory.get.binaryDecoder(bytes, null)
    try {
      reader.read(null.asInstanceOf[T], decoder)
    } catch {
      case _: Exception => null.asInstanceOf[T]
    }
  }

  def recordDecode[T <: SpecificRecordBase](bytes: Array[Byte]): T = {
    import com.madhouse.ssp.entity.LogType._
    val record = Configure.logType match {
      case MEDIABID => recordDecode[MediaBid](bytes, MediaBid.SCHEMA$)
      case DSPBID => recordDecode[DSPBid](bytes, DSPBid.SCHEMA$)
      case IMPRESSION => recordDecode[ImpressionTrack](bytes, ImpressionTrack.SCHEMA$)
      case CLICK => recordDecode[ClickTrack](bytes, ClickTrack.SCHEMA$)
      case _ => null
    }
    record.asInstanceOf[T]
  }
}