package com.ksr.dataflow.exceptions

case class DataFlowInvalidFileException(private val message: String = "",
                                        private val cause: Throwable = None.get)
  extends Exception(message, cause)
