package com.ksr.dataflow.configuration.job.input

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration
import com.ksr.dataflow.configuration.job.InputConfig
import com.ksr.dataflow.exceptions.DataFlowException
import com.ksr.dataflow.input.Reader
import com.ksr.dataflow.input.readers.azure.AzureBlobStorageInput

case class Azure(@JsonScalaEnumeration(classOf[StorageTypeReference]) storageType: StorageType.StorageType,
                 containerName: String,
                 storageAccountName: String,
                 blob_sas_token: String,
                 blob_relative_path: String,
                 options: Option[Map[String, String]],
                 format: Option[String]) extends InputConfig {
  
  override def getReader(name: String): Reader = storageType match {
    case StorageType.AzureBlobStorage =>
      AzureBlobStorageInput(name, containerName, storageAccountName, blob_sas_token, blob_relative_path, options, format)
    case _ =>  throw new DataFlowException(s"Not Supported Writer ${storageType}")
  }

}

object StorageType extends Enumeration {
  type StorageType = Value

  val AzureBlobStorage = Value

}

class StorageTypeReference extends TypeReference[StorageType.type]
