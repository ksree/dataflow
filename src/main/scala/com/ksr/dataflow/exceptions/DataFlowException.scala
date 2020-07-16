package com.ksr.dataflow.exceptions

case class DataFlowException(private val message: String = "",
                             private val cause: Throwable = None.orNull)
  extends Exception(message, cause)