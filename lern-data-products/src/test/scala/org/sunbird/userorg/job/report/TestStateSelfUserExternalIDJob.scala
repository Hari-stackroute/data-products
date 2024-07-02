package org.sunbird.userorg.job.report


import okhttp3.mockwebserver.{Dispatcher, MockResponse, MockWebServer, RecordedRequest}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.util.JSONUtils.serialize
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{Fetcher, FrameworkContext, JobConfig, StorageConfig}
import org.scalamock.matchers.Matchers
import org.scalamock.scalatest.MockFactory
import org.sunbird.core.util.{EmbeddedCassandra, HTTPResponse}
import org.sunbird.lms.job.report.{BaseReportSpec, BaseReportsJob}
import org.sunbird.userorg.job.report.StateAdminReportJob.{container, getStorageConfig, objectKey}

class TestStateSelfUserExternalIDJob extends BaseReportSpec with Matchers with MockFactory {

  implicit var spark: SparkSession = _
  var map: Map[String, String] = _
  var orgDF: DataFrame = _
  var reporterMock: BaseReportsJob = mock[BaseReportsJob]
  val sunbirdKeyspace = "sunbird"
  var storageConfig: StorageConfig = _
  val tenantPrefWebserver = new MockWebServer()

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();
    val modelParams = Map[String, AnyRef]("adhoc_scripts_virtualenv_dir" -> "/mount/venv",
      "adhoc_scripts_output_dir" -> "/mount/portal_data")

    val jobConfig = JobConfig(Fetcher("local", None, None), None, None, "StateAdminJob", Some(modelParams), None, Some(4), Some("TestExecuteDispatchder"))
    storageConfig = getStorageConfig(container, objectKey, jobConfig);
    EmbeddedCassandra.loadData("src/test/resources/reports/user_self_test_data.cql") // Load test data in embedded cassandra server
    //Staring MockWebServer
    val tenantPrefDispatcher: Dispatcher = new Dispatcher() {
      @throws[InterruptedException]
      override def dispatch(request: RecordedRequest): MockResponse = {
        val body = new String(request.getBody.readByteArray())
        val jsonBody = JSONUtils.deserialize[Map[String, AnyRef]](body)
        val requestBody = jsonBody.getOrElse("request", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("key", "")
        (request.getPath, request.getMethod, requestBody) match {
          case ("/private/v2/org/preferences/read", "POST", "dataSecurityPolicy") =>
            new MockResponse().setHeader("Content-Type", "application/json").setResponseCode(200).setBody("""{"id":".private.v2.org.preferences.read","ver":"private","ts":"2023-05-26 16:25:42:913+0000","params":{"resmsgid":"976058ce-570a-4c56-a5f9-623141bedd4a","msgid":"976058ce-570a-4c56-a5f9-623141bedd4a","err":null,"status":"SUCCESS","errmsg":null},"responseCode":"OK","result":{"response":{"updatedBy":"fbe926ac-a395-40e4-a65b-9b4f711d7642","data":{"level":"PLAIN_DATASET","dataEncrypted":"No","comments":"Data is not encrypted","job":{"progress-exhaust":{"level":"PUBLIC_KEY_ENCRYPTED_DATASET","dataEncrypted":"No","comments":"Password protected file."},"response-exhaust":{"level":"TEXT_KEY_ENCRYPTED_DATASET","dataEncrypted":"No","comments":"Password protected file."},"userinfo-exhaust":{"level":"PASSWORD_PROTECTED_DATASET","dataEncrypted":"No","comments":"Password protected file."},"program-user-exhaust":{"level":"TEXT_KEY_ENCRYPTED_DATASET","dataEncrypted":"Yes","comments":"Text key Encrypted File"}},"securityLevels":{"PLAIN_DATASET":"Data is present in plain text/zip. Generally applicable to open datasets.","PASSWORD_PROTECTED_DATASET":"Password protected zip file. Generally applicable to non PII data sets but can contain sensitive information which may not be considered open.","TEXT_KEY_ENCRYPTED_DATASET":"Data encrypted with a user provided encryption key. Generally applicable to non PII data but can contain sensitive information which may not be considered open.","PUBLIC_KEY_ENCRYPTED_DATASET":"Data encrypted via an org provided public/private key. Generally applicable to all PII data exhaust."}},"createdBy":"fbe926ac-a395-40e4-a65b-9b4f711d7642","updatedOn":1684825029544,"createdOn":1682501851315,"orgId":"default","key":"dataSecurityPolicy"}}}""")
        }
      }
    }
    tenantPrefWebserver.setDispatcher(tenantPrefDispatcher)
    tenantPrefWebserver.start(9090)
  }
  
  override def afterAll() : Unit = {
    super.afterAll();
    (new HadoopFileUtil()).delete(spark.sparkContext.hadoopConfiguration, "src/test/resources/admin-user-reports")
    tenantPrefWebserver.shutdown()
  }

  //Created data : channels ApSlug and OtherSlug contains validated users created against blocks,districts and state
  //Only TnSlug doesn't contain any validated users
  /*"StateSelfUserExternalID" should "generate reports" in {*/
    ignore should "generate reports" in {
    implicit val fc = new FrameworkContext()
    val reportDF = StateAdminReportJob.generateExternalIdReport()(spark, fc)
    assert(reportDF.count() === 2);

    val user1 = reportDF.filter(col("Sunbird UUID") === "56c2d9a3-fae9-4341-9862-4eeeead2e9a1").first

    user1.getAs[String]("Profile Email") should be ("PEhQxQlaMdJEXOzShY0NAiKg4LqC2xUDE4InNodhG/fJMhq69iAPzseEdYAlMPWegxJaAnH+tJwc\\nZuqPxJCtJkiGfwlCUEj5B41z4/RjH/7XowwzRVZXH0jth3IW4Ik8TQtMGOn7lhkDdxs1iV8l8A==")
    user1.getAs[String]("User Type") should be ("administrator,teacher,other,parent")
    user1.getAs[String]("Profile Phone number") should be ("1wsQrmy8Q1T4gFa+MOJsirdQC2yhyJsm2Rgj229s2b5Hk/JLNNnHMz6ywhgzYpgcQ6QILjcTLl7z\\n7s4aRbsrWw==")
    user1.getAs[String]("Name") should be ("localuser118f localuser118l")
    user1.getAs[String]("provider") should be ("ap")
    user1.getAs[String]("State provided ext. ID") should be (null)
    user1.getAs[String]("Org Phone") should be ("1wsQrmy8Q1T4gFa+MOJsirdQC2yhyJsm2Rgj229s2b5Hk/JLNNnHMz6ywhgzYpgcQ6QILjcTLl7z\\n7s4aRbsrWw==")
    user1.getAs[String]("School UDISE ID") should be ("190923")
    user1.getAs[String]("School Name") should be ("mgm21")
    user1.getAs[String]("District") should be ("Chittooor")
    user1.getAs[String]("Org Email ID") should be ("PEhQxQlaMdJEXOzShY0NAiKg4LqC2xUDE4InNodhG/fJMhq69iAPzseEdYAlMPWegxJaAnH+tJwc\\nZuqPxJCtJkiGfwlCUEj5B41z4/RjH/7XowwzRVZXH0jth3IW4Ik8TQtMGOn7lhkDdxs1iV8l8A==")
    user1.getAs[String]("Root Org of user") should be ("AP")
    user1.getAs[String]("State") should be ("Andhra")
    user1.getAs[String]("User-Sub Type") should be ("hm,crp")
    user1.getAs[String]("Cluster") should be ("Chittooorblock1cluster1")
    user1.getAs[String]("Block") should be ("Chittooorblock1")

    val user2 = reportDF.filter(col("Sunbird UUID") === "8eaa1621-ac15-42a4-9e26-9c846963f331").first

    user2.getAs[String]("Profile Email") should be ("PEhQxQlaMdJEXOzShY0NAiKg4LqC2xUDE4InNodhG/fJMhq69iAPzseEdYAlMPWegxJaAnH+tJwc\\nZuqPxJCtJkiGfwlCUEj5B41z4/RjH/7XowwzRVZXH0jth3IW4Ik8TQtMGOn7lhkDdxs1iV8l8A==")
    user2.getAs[String]("User Type") should be ("teacher")
    user2.getAs[String]("Profile Phone number") should be ("1wsQrmy8Q1T4gFa+MOJsirdQC2yhyJsm2Rgj229s2b5Hk/JLNNnHMz6ywhgzYpgcQ6QILjcTLl7z\\n7s4aRbsrWw==")
    user2.getAs[String]("Name") should be ("localuser117f localuser117l")
    user2.getAs[String]("provider") should be ("ka")
    user2.getAs[String]("State provided ext. ID") should be (null)
    user2.getAs[String]("Org Phone") should be ("1wsQrmy8Q1T4gFa+MOJsirdQC2yhyJsm2Rgj229s2b5Hk/JLNNnHMz6ywhgzYpgcQ6QILjcTLl7z\\n7s4aRbsrWw==")
    user2.getAs[String]("School UDISE ID") should be ("orgext2")
    user2.getAs[String]("School Name") should be ("mgm21")
    user2.getAs[String]("Sunbird UUID") should be ("8eaa1621-ac15-42a4-9e26-9c846963f331")
    user2.getAs[String]("District") should be ("Gulbarga")
    user2.getAs[String]("Org Email ID") should be ("PEhQxQlaMdJEXOzShY0NAiKg4LqC2xUDE4InNodhG/fJMhq69iAPzseEdYAlMPWegxJaAnH+tJwc\\nZuqPxJCtJkiGfwlCUEj5B41z4/RjH/7XowwzRVZXH0jth3IW4Ik8TQtMGOn7lhkDdxs1iV8l8A==")
    user2.getAs[String]("Root Org of user") should be ("MPPS SIMHACHALNAGAR")
    user2.getAs[String]("State") should be ("Karnataka")
    user2.getAs[String]("User-Sub Type") should be("")
    user2.getAs[String]("Cluster") should be ("Gulbargablockcluster1")
    user2.getAs[String]("Block") should be ("Gulbargablock1")

  }

  ignore should "execute with zip failed to generate" in {
    /*"StateSelfUserExternalIDWithZip" should "execute with zip failed to generate" in {*/
    implicit val fc = new FrameworkContext()
    try {
      val l3LevelRespponse = createHTTPResponse("TEXT_KEY_ENCRYPTED_DATASET")
      val reportDF = StateAdminReportJob.generateExternalIdReport()(spark, fc)
    } catch {
      case ex: Exception => assert(ex.getMessage === "Self-Declared user level zip generation failed with exit code 127");
    }
  }

  "StateAdminReportJob" should "execute main method" in {
    implicit val fc = new FrameworkContext()
    val modelParams = Map[String, AnyRef]("adhoc_scripts_virtualenv_dir" -> "/mount/venv",
      "adhoc_scripts_output_dir" -> "/mount/portal_data")

    val jobConfig = JobConfig(Fetcher("local", None, None), None, None, "StateAdminJob", Some(modelParams), None, Some(4), Some("TestExecuteDispatchder"))
    val strConfig = JSONUtils.serialize(jobConfig)
    StateAdminReportJob.main(strConfig)
  }

  def createResponseBody(level: String) : String = {
    val jobData = Map[String, AnyRef]("admin-user-reports" -> level)
    val dataMap = Map[String, AnyRef]("level" -> "PLAIN_DATASET", "job" -> jobData)
    val responseMap = Map[String, AnyRef]("data" -> dataMap)
    val resultMap = Map[String, AnyRef]("response" -> responseMap)
    val responseBodyMap = Map[String, AnyRef]("result" -> resultMap)
    val responseBodyStr = serialize(responseBodyMap)
    responseBodyStr
  }

  def createHTTPResponse(level: String) : HTTPResponse = {
    val responseBody = createResponseBody(level)
    val httpResponse = HTTPResponse(200, responseBody)
    httpResponse
  }
}
