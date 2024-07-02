package org.sunbird.lms.exhaust.collection

import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.sunbird.core.exhaust.JobRequest

object UserInfoExhaustJob extends BaseCollectionExhaustJob with Serializable {

  /*
  * * Available columns for the report
  *
  * "courseid", "collectionName", "batchid", "batchName", "userid", "username", "state", "district", "orgname", "email", "phone",
    "consentflag", "consentprovideddate", "block", "cluster", "usertype", "usersubtype", "schooludisecode", "schoolname"
  * */

  override def getClassName = "org.sunbird.lms.exhaust.collection.UserInfoExhaustJob"

  override def jobName() = "UserInfoExhaustJob";

  override def jobId() = "userinfo-exhaust";

  override def getReportPath() = "userinfo-exhaust/";

  override def getReportKey() = "userinfo";
  private val encryptedFields = Array("email", "phone", "username");

  override def getUserCacheColumns(): Seq[String] = {
    Seq("userid", "username", "state", "district", "rootorgid", "orgname", "email", "phone", "block", "cluster", "usertype", "usersubtype", "schooludisecode", "schoolname", "status")
  }

  override def validateRequest(request: JobRequest): Boolean = {
    if (super.validateRequest(request)) {
      if (request.encryption_key.isDefined) true else false;
    } else {
      false;
    }
  }

  /*private val filterColumns = Seq("courseid", "collectionName", "batchid", "batchName", "userid", "username", "state", "district", "orgname", "email", "phone",
    "consentflag", "consentprovideddate", "block", "cluster", "usertype", "usersubtype", "schooludisecode", "schoolname");*/

  private val consentFields = List("email", "phone")
  private val orgDerivedFields = List("username")

  override def processBatch(userEnrolmentDF: DataFrame, collectionBatch: CollectionBatch)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    collectionBatch.userConsent.getOrElse("No").toLowerCase() match {
      case "yes" =>
        val unmaskedDF = decryptUserInfo(applyConsentRules(collectionBatch, userEnrolmentDF))
        val reportDF = unmaskedDF.select(reportColumnList.head, reportColumnList.tail: _*);
        organizeDF(reportDF, reportColumnMapping, reportColumnMapping.values.toList)

      case _ =>
        throw new Exception("Invalid request. User info exhaust is not applicable for collections which don't request for user consent to share data")
    }
  }

  def applyConsentRules(collectionBatch: CollectionBatch, userDF: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val consentDF = if (collectionBatch.requestedOrgId.equals(collectionBatch.custodianOrgId)) {
      userDF.withColumn("consentflag", lit("false"));
    } else {
      val consentDF = getUserConsentDF(collectionBatch);
      val resultDF = userDF.join(consentDF, Seq("userid"), "inner")
      // Org level consent - will be updated in 3.4 to read from user_consent table
      resultDF.withColumn("orgconsentflag", when(col("rootorgid") === collectionBatch.requestedOrgId, "true").otherwise("false"))
    }
    // Issue #SB-24966: Logic to exclude users whose consentflag is false
    consentDF.filter(col("consentflag") === "true")
  }

  def decryptUserInfo(userDF: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val schema = userDF.schema
    val decryptFields = schema.fields.filter(field => encryptedFields.contains(field.name));
    val resultDF = decryptFields.foldLeft(userDF)((df, field) => {
      df.withColumn(field.name, when(col("consentflag") === "true", UDFUtils.toDecrypt(col(field.name))).otherwise(col(field.name)))
    })
    resultDF
  }

  /**
   * UserInfo Exhaust should be an encrypted file. So, don't ignore zip and encryption exceptions.
   *
   * @return
   */
  override def canZipExceptionBeIgnored(): Boolean = false

  override def validateCsvColumns(piiFields: List[String], csvColumns: List[String], level: String): Boolean = {
    var exists = false
    if(level == "PASSWORD_PROTECTED_DATASET" || level == "TEXT_KEY_ENCRYPTED_DATASET" || level == "PUBLIC_KEY_ENCRYPTED_DATASET") {
      for(encField <- encryptedFields) {
        if(!(csvColumns.contains(encField)) || !(piiFields.contains(encField))) {
          return exists
        }
        exists =  true
      }
    }
    exists
  }

}
