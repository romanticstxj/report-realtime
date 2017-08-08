package com.madhouse.ssp.entity

/**
  * Created by Sunxiang on 2017-07-28 09:50.
  *
  */
object LogType extends Enumeration {
  type LogType  = Value
  val MEDIABID, DSPBID, IMPRESSION, CLICK = Value
}