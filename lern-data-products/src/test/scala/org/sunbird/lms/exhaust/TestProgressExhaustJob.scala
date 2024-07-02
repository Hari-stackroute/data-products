package org.sunbird.lms.exhaust

import okhttp3.mockwebserver.{Dispatcher, MockResponse, MockWebServer, RecordedRequest}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Encoders, SQLContext, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.core.exhaust.BaseReportsJob
import org.sunbird.core.util.{EmbeddedCassandra, EmbeddedPostgresql, RedisConnect}
import org.sunbird.lms.exhaust.collection.{AssessmentData, CollectionBatch, CourseData, Metrics, ProgressExhaustJob}
import org.sunbird.lms.job.report.BaseReportSpec
import redis.embedded.RedisServer

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.JavaConverters._
case class ProgressExhaustReport(`Collection Id`: String, `Collection Name`: String, `Batch Id`: String, `Batch Name`: String, `User UUID`: String, `State`: String, `District`: String, `Org Name`: String,
                                 `School Id`: String, `School Name`: String, `Block Name`: String, `Declared Board`: String, `Enrolment Date`: String, `Completion Date`: String, `Certificate Status`: String, `Progress`: String,
                                 `Total Score`: String, `Cluster Name`: String, `User Type`: String, `User Sub Type`: String)
case class ContentHierarchy(identifier: String, hierarchy: String)

class TestProgressExhaustJob extends BaseReportSpec with MockFactory with BaseReportsJob {

  val jobRequestTable = "job_request"
  implicit var spark: SparkSession = _
  var redisServer: RedisServer = _
  val tenantPrefWebserver = new MockWebServer()
  val tenantPrefWebserver1 = new MockWebServer()
  override def beforeAll(): Unit = {
    spark = getSparkSession();
    super.beforeAll()
    redisServer = new RedisServer(6341)
    redisServer.start()
    setupRedisData()
    EmbeddedCassandra.loadData("src/test/resources/exhaust/report_data.cql") // Load test data in embedded cassandra server
    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createJobRequestTable()

    val tenantPrefDispatcher: Dispatcher = new Dispatcher() {
      @throws[InterruptedException]
      override def dispatch(request: RecordedRequest): MockResponse = {
        val body = new String(request.getBody.readByteArray())
        val jsonBody = JSONUtils.deserialize[Map[String, AnyRef]](body)
        val requestBody = jsonBody.getOrElse("request", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("key", "")
        val jsonBody1 = JSONUtils.deserialize[Map[String, AnyRef]](body)
        val requestBody1 = jsonBody1.getOrElse("request", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("filters", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("id", "")
        (request.getPath, request.getMethod, requestBody) match {
          case ("/private/v2/org/preferences/read", "POST", "dataSecurityPolicy") =>
            new MockResponse().setHeader("Content-Type", "application/json").setResponseCode(200).setBody("""{"id":".private.v2.org.preferences.read","ver":"private","ts":"2023-05-26 16:25:42:913+0000","params":{"resmsgid":"976058ce-570a-4c56-a5f9-623141bedd4a","msgid":"976058ce-570a-4c56-a5f9-623141bedd4a","err":null,"status":"SUCCESS","errmsg":null},"responseCode":"OK","result":{"response":{"updatedBy":"fbe926ac-a395-40e4-a65b-9b4f711d7642","data":{"level":"PLAIN_DATASET","dataEncrypted":"No","comments":"Data is not encrypted","job":{"progress-exhaust":{"level":"TEXT_KEY_ENCRYPTED_DATASET","dataEncrypted":"No","comments":"Password protected file."},"response-exhaust":{"level":"PUBLIC_KEY_ENCRYPTED_DATASET","dataEncrypted":"No","comments":"Password protected file."},"userinfo-exhaust":{"level":"PASSWORD_PROTECTED_DATASET","dataEncrypted":"No","comments":"Password protected file."},"program-user-exhaust":{"level":"TEXT_KEY_ENCRYPTED_DATASET","dataEncrypted":"Yes","comments":"Text key Encrypted File"}},"securityLevels":{"PLAIN_DATASET":"Data is present in plain text/zip. Generally applicable to open datasets.","PASSWORD_PROTECTED_DATASET":"Password protected zip file. Generally applicable to non PII data sets but can contain sensitive information which may not be considered open.","TEXT_KEY_ENCRYPTED_DATASET":"Data encrypted with a user provided encryption key. Generally applicable to non PII data but can contain sensitive information which may not be considered open.","PUBLIC_KEY_ENCRYPTED_DATASET":"Data encrypted via an org provided public/private key. Generally applicable to all PII data exhaust."}},"createdBy":"fbe926ac-a395-40e4-a65b-9b4f711d7642","updatedOn":1684825029544,"createdOn":1682501851315,"orgId":"default","key":"dataSecurityPolicy"}}}""")
          case ("/private/v2/org/preferences/read", "POST", "userPrivateFields") =>
            new MockResponse().setHeader("Content-Type", "application/json").setResponseCode(200).setBody("""{"id":".private.v2.org.preferences.read","ver":"private","ts":"2023-05-26 16:25:42:913+0000","params":{"resmsgid":"976058ce-570a-4c56-a5f9-623141bedd4a","msgid":"976058ce-570a-4c56-a5f9-623141bedd4a","err":null,"status":"SUCCESS","errmsg":null},"responseCode":"OK","result":{"response":{"data": {"piiFields":["courseid", "collectionName", "batchid", "batchName", "userid",  "state", "district", "orgname", "schooludisecode", "schoolname", "board", "block", "cluster", "usertype", "usersubtype", "enrolleddate", "completedon", "certificatestatus", "completionPercentage"]}}}}""")
         }
      }
    }
    val tenantPrefDispatcher1: Dispatcher = new Dispatcher() {
      @throws[InterruptedException]
      override def dispatch(request: RecordedRequest): MockResponse = {
        val body = new String(request.getBody.readByteArray())
        val jsonBody1 = JSONUtils.deserialize[Map[String, AnyRef]](body)
        val requestBody1 = jsonBody1.getOrElse("request", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("filters", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("id", "")
        (request.getPath, request.getMethod, requestBody1) match {
          case ("/private/v2/org/search", "POST", "b00bc992ef25f1a9a8d63291e20efc8d") =>
            new MockResponse().setHeader("Content-Type", "application/json").setResponseCode(200).setBody("""{"id":".private.v2.org.search","ver":"private","ts":"2023-06-19 06:04:15:027+0000","params":{"resmsgid":"ab21f473-c0ba-4bcb-90f6-cd5f86853f7a","msgid":"ab21f473-c0ba-4bcb-90f6-cd5f86853f7a","err":null,"status":"SUCCESS","errmsg":null},"responseCode":"OK","result":{"response":{"count":1,"content":[{"keys":{"exhaustEncryptionKey":["https://sunbirddevbbpublic.blob.core.windows.net/sunbird-content-dev/organisation/01269878797503692810/namratha_public.pem"]},"channel":"tn","description":"Tamil Nadu","updatedDate":"2022-02-18 10:07:57:360+0000","organisationType":2,"isTenant":true,"provider":null,"id":"01269878797503692810","email":null,"slug":"tn","isSSOEnabled":null,"identifier":"01269878797503692810","orgName":"Tamil Nadu","updatedBy":"fbe926ac-a395-40e4-a65b-9b4f711d7642","externalId":"tn001","orgLocation":[],"isRootOrg":true,"rootOrgId":"01269878797503692810","imgUrl":null,"isSchool":true,"homeUrl":null,"createdDate":"2019-02-14 11:41:31:097+0000","createdBy":null,"hashTagId":"01269878797503692810","status":1}]}}}""")
        }
      }
    }
    tenantPrefWebserver.setDispatcher(tenantPrefDispatcher)
    tenantPrefWebserver.start(9090)
    tenantPrefWebserver1.setDispatcher(tenantPrefDispatcher1)
    tenantPrefWebserver1.start(9091)
  }

  override def afterAll() : Unit = {
    super.afterAll()
    redisServer.stop()
    println("******** closing the redis connection **********" + redisServer.isActive)
    EmbeddedCassandra.close()
    EmbeddedPostgresql.close()
    spark.close()
    tenantPrefWebserver.close()
    tenantPrefWebserver1.close()
  }

  def setupRedisData(): Unit = {
    val redisConnect = new RedisConnect("localhost", 6341)
    val jedis = redisConnect.getConnection(0, 100000)
    jedis.hmset("user:user-001", JSONUtils.deserialize[java.util.Map[String, String]]("""{"cluster":"CLUSTER1","firstname":"Manju","subject":"[\"IRCS\"]","schooludisecode":"3183211","usertype":"administrator","usersignintype":"Validated","language":"[\"English\"]","medium":"[\"English\"]","userid":"a962a4ff-b5b5-46ad-a9fa-f54edf1bcccb","schoolname":"DPS, MATHURA","rootorgid":"01250894314817126443","lastname":"Kapoor","framework_id":"sunbird_health","orgname":"Root Org2","phone":"","usersubtype":"deo","district":"bengaluru","grade":"[\"Volunteers\"]","block":"BLOCK1","state":"Karnataka","framework_board":"[\"Sunbird-Health\"]","email":""};"""))
    jedis.hmset("user:user-002", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Mahesh", "userid": "user-002", "state": "Andhra Pradesh", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "mahesh@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-003", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Sowmya", "userid": "user-003","usertype":"administrator", "usersubtype":"deo", "cluster": "anagha" ,"state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "sowmya@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-004", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Utkarsha", "userid": "user-004", "state": "Delhi", "district": "babarpur", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "utkarsha@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-005", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Isha", "userid": "user-005", "state": "MP", "district": "Jhansi", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "isha@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-006", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Revathi", "userid": "user-006", "state": "Andhra Pradesh", "district": "babarpur", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "revathi@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-007", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Sunil", "userid": "user-007", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0126391644091351040", "email": "sunil@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-008", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Anoop", "userid": "user-008", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "anoop@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-009", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Kartheek", "userid": "user-009", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "01285019302823526477", "email": "kartheekp@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-010", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Anand", "userid": "user-010", "state": "Tamil Nadu", "district": "Chennai", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "anandp@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-011", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Santhosh", "userid": "user-011", "state": "Tamil Nadu", "district": "Chennai", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "anandp@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-012", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Rayulu", "userid": "user-012","usertype":"administrator", "usersubtype":"deo", "cluster": "anagha", "state": "Tamil Nadu", "district": "Chennai", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "anandp@ilimi.in", "usersignintype": "Validated"};"""))

    jedis.close()
  }

  "ProgressExhaustReport" should "generate the report with all the correct data" in {

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130928636168192001667_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'progress-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-001\"}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.lms.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"localhost","sparkUserDbRedisPort":6341,"sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":"","userCacheCols":["framework_board"],"columnMapping":{"courseid":"Collection Id","collectionName":"Collection Name","batchid":"Batch Id","batchName":"Batch Name","userid":"User UUID","state":"State","district":"District","orgname":"Org Name","schooludisecode":"School Id","schoolname":"School Name","block":"Block Name","cluster":"Cluster Name","usertype":"User Type","usersubtype":"User Sub Type","framework_board":"Declared Board","enrolleddate":"Enrolment Date","completedon":"Completion Date","certificatestatus":"Certificate Status","completionPercentage":"Progress","total_sum_score":"Total Score"},"csvColumns":["courseid", "collectionName", "batchid", "batchName", "userid",  "state", "district", "orgname", "schooludisecode", "schoolname", "framework_board", "block", "cluster", "usertype", "usersubtype", "enrolleddate", "completedon", "certificatestatus", "completionPercentage"]},"parallelization":8,"appName":"Progress Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    val metrics:Metrics = ProgressExhaustJob.execute()
    metrics.totalRequests.getOrElse(0) should be(1)
    metrics.failedRequests.getOrElse(0) should be(0)
    metrics.successRequests.getOrElse(0) should be(1)
    metrics.duplicateRequests.getOrElse(0) should be(0)

    val outputLocation = AppConf.getConfig("collection.exhaust.store.prefix")
    val outputDir = "progress-exhaust"
    val batch1 = "batch-001"
    val requestId = "37564CF8F134EE7532F125651B51D17F"
    val filePath = ProgressExhaustJob.getFilePath(batch1, requestId)
    val jobName = ProgressExhaustJob.jobName()

    implicit val responseExhaustEncoder = Encoders.product[ProgressExhaustReport]
    val batch1Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$filePath.csv").as[ProgressExhaustReport].collectAsList().asScala

    batch1Results.size should be (4)
    batch1Results.map(f => f.`Collection Id`).toList should contain atLeastOneElementOf List("do_1130928636168192001667")
    batch1Results.map(f => f.`Collection Name`).toList should contain atLeastOneElementOf List("24 aug course")
    batch1Results.map(f => f.`Batch Id`).toList should contain atLeastOneElementOf List("BatchId_batch-001")
    batch1Results.map(f => f.`Batch Name`).toList should contain atLeastOneElementOf List("Basic Java")
    batch1Results.map {res => res.`User UUID`}.toList should contain theSameElementsAs List("user-001", "user-002", "user-003", "user-004")
    batch1Results.map {res => res.`State`}.toList should contain theSameElementsAs List("Karnataka", "Andhra Pradesh", "Karnataka", "Delhi")
    batch1Results.map {res => res.`District`}.toList should contain theSameElementsAs List("bengaluru", "bengaluru", "bengaluru", "babarpur")
    batch1Results.map(f => f.`Enrolment Date`).toList should contain allElementsOf  List("15/11/2019")
    batch1Results.map(f => f.`Completion Date`).toList should contain allElementsOf  List(null)
    batch1Results.map(f => f.`Progress`).toList should contain allElementsOf  List("100")
    batch1Results.map(f => f.`Cluster Name`).toList should contain atLeastOneElementOf List("CLUSTER1")
    batch1Results.map(f => f.`User Type`).toList should contain atLeastOneElementOf List("administrator")
    batch1Results.map(f => f.`User Sub Type`).toList should contain atLeastOneElementOf List("deo")

    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='progress-exhaust'")
    val reportDate = getDate("yyyyMMdd").format(Calendar.getInstance().getTime())

    while(pResponse.next()) {
      pResponse.getString("status") should be ("SUCCESS")
      pResponse.getString("err_message") should be ("")
      pResponse.getString("dt_job_submitted") should be ("2020-10-19 05:58:18.666")
      pResponse.getString("download_urls") should be (s"""{progress-exhaust/$requestId/batch-001_progress_${reportDate}.zip}""")
      pResponse.getString("dt_file_created") should be (null)
      pResponse.getString("iteration") should be ("0")
    }

    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, outputLocation)

    //Test coverage for filterAssessmentsFromHierarchy method
    val assessmentData = ProgressExhaustJob.filterAssessmentsFromHierarchy(List(), Map(), AssessmentData("do_1130928636168192001667", List()))
    assessmentData.courseid should be ("do_1130928636168192001667")
    assert(assessmentData.assessmentIds.isEmpty)

  }

  it should "test the exhaust report file size limits and stop request in between" in {

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130928636168192001667_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'progress-exhaust', 'SUBMITTED', '{\"batchFilter\": [\"batch-001\",\"batch-004\"]}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")
    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.lms.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"localhost","sparkUserDbRedisPort":6341,"sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":"","userCacheCols":["framework_board"],"columnMapping":{"courseid":"Collection Id","collectionName":"Collection Name","batchid":"Batch Id","batchName":"Batch Name","userid":"User UUID","state":"State","district":"District","orgname":"Org Name","schooludisecode":"School Id","schoolname":"School Name","block":"Block Name","cluster":"Cluster Name","usertype":"User Type","usersubtype":"User Sub Type","framework_board":"Declared Board","enrolleddate":"Enrolment Date","completedon":"Completion Date","certificatestatus":"Certificate Status","completionPercentage":"Progress","total_sum_score":"Total Score"},"csvColumns":["courseid", "collectionName", "batchid", "batchName", "userid",  "state", "district", "orgname", "schooludisecode", "schoolname", "framework_board", "block", "cluster", "usertype", "usersubtype", "enrolleddate", "completedon", "certificatestatus", "completionPercentage"]},"parallelization":8,"appName":"Progress Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    ProgressExhaustJob.execute()

    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='progress-exhaust'")
    val reportDate = getDate("yyyyMMdd").format(Calendar.getInstance().getTime())

    while(pResponse.next()) {
      pResponse.getString("status") should be ("SUBMITTED")
      pResponse.getString("download_urls") should be ("{}")
      pResponse.getString("processed_batches") should not be (null)
    }
  }

  it should "test the exhaust report on limits with previously completed request" in {

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key,processed_batches) VALUES ('do_1130928636168192001667_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'progress-exhaust', 'SUBMITTED', '{\"batchFilter\": [\"batch-001\",\"batch-004\"]}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12','[{\"batchId\":\"batch-001\",\"filePath\":\"progress-exhaust/37564CF8F134EE7532F125651B51D17F/batch-001_progress_20210509.zip\",\"fileSize\":0}]');")
    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.lms.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"localhost","sparkUserDbRedisPort":6341,"sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":"","userCacheCols":["framework_board"],"columnMapping":{"courseid":"Collection Id","collectionName":"Collection Name","batchid":"Batch Id","batchName":"Batch Name","userid":"User UUID","state":"State","district":"District","orgname":"Org Name","schooludisecode":"School Id","schoolname":"School Name","block":"Block Name","cluster":"Cluster Name","usertype":"User Type","usersubtype":"User Sub Type","framework_board":"Declared Board","enrolleddate":"Enrolment Date","completedon":"Completion Date","certificatestatus":"Certificate Status","completionPercentage":"Progress","total_sum_score":"Total Score"},"csvColumns":["courseid", "collectionName", "batchid", "batchName", "userid",  "state", "district", "orgname", "schooludisecode", "schoolname", "framework_board", "block", "cluster", "usertype", "usersubtype", "enrolleddate", "completedon", "certificatestatus", "completionPercentage"]},"parallelization":8,"appName":"Progress Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    ProgressExhaustJob.execute()

    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='progress-exhaust'")
    val reportDate = getDate("yyyyMMdd").format(Calendar.getInstance().getTime())

    while(pResponse.next()) {
      pResponse.getString("status") should be ("SUCCESS")
      pResponse.getString("download_urls") should not be (null)
      pResponse.getString("processed_batches") should not be (null)
      pResponse.getString("download_urls") should be (s"""{progress-exhaust/37564CF8F134EE7532F125651B51D17F/batch-001_progress_20210509.zip,progress-exhaust/37564CF8F134EE7532F125651B51D17F/batch-004_progress_${reportDate}.zip}""")
    }
  }

  it should "test the exhaust report with batches limit by channel and stop request in between" in {

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130928636168192001667_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F-3', 'progress-exhaust', 'SUBMITTED', '{\"batchFilter\": [\"batch-004\", \"batch-005\"]}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130928636168192001667_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F-4', 'progress-exhaust', 'SUBMITTED', '{\"batchFilter\": [\"batch-002\", \"batch-003\"]}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130928636168192001667_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F-1', 'progress-exhaust', 'SUBMITTED', '{\"batchFilter\": [\"batch-004\", \"batch-002\"]}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130928636168192001667_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F-2', 'progress-exhaust', 'SUBMITTED', '{\"batchFilter\": [\"batch-003\", \"batch-004\"]}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.lms.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"localhost","sparkUserDbRedisPort":6341,"sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":"","userCacheCols":["framework_board"],"columnMapping":{"courseid":"Collection Id","collectionName":"Collection Name","batchid":"Batch Id","batchName":"Batch Name","userid":"User UUID","state":"State","district":"District","orgname":"Org Name","schooludisecode":"School Id","schoolname":"School Name","block":"Block Name","cluster":"Cluster Name","usertype":"User Type","usersubtype":"User Sub Type","framework_board":"Declared Board","enrolleddate":"Enrolment Date","completedon":"Completion Date","certificatestatus":"Certificate Status","completionPercentage":"Progress","total_sum_score":"Total Score"},"csvColumns":["courseid", "collectionName", "batchid", "batchName", "userid",  "state", "district", "orgname", "schooludisecode", "schoolname", "framework_board", "block", "cluster", "usertype", "usersubtype", "enrolleddate", "completedon", "certificatestatus", "completionPercentage"]},"parallelization":8,"appName":"Progress Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    ProgressExhaustJob.execute()

    Thread.sleep(5000)

    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='progress-exhaust' and request_id='37564CF8F134EE7532F125651B51D17F-4'")

    while(pResponse.next()) {
      pResponse.getString("status") should be ("SUBMITTED")
      pResponse.getString("download_urls") should be ("{}")
      pResponse.getString("processed_batches") should not be (null)
    }

    val pResponse2 = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='progress-exhaust' and request_id='37564CF8F134EE7532F125651B51D17F-3'")
    val reportDate = getDate("yyyyMMdd").format(Calendar.getInstance().getTime)

    while(pResponse2.next()) {
      pResponse2.getString("status") should be ("SUCCESS")
      pResponse2.getString("download_urls") should be (s"{progress-exhaust/37564CF8F134EE7532F125651B51D17F-3/batch-004_progress_${reportDate}.zip}")
      pResponse2.getString("processed_batches") should not be (null)
    }
  }

  it should "test the exhaust report file size limit by channel and stop request in between" in {

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130928636168192001667_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F-2', 'progress-exhaust', 'SUBMITTED', '{\"batchFilter\": [\"batch-001\"]}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130928636168192001667_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F-3', 'progress-exhaust', 'SUBMITTED', '{\"batchFilter\": [\"batch-002\"]}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130928636168192001667_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F-1', 'progress-exhaust', 'SUBMITTED', '{\"batchFilter\": [\"batch-004\"]}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.lms.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"localhost","sparkUserDbRedisPort":6341,"sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":"","userCacheCols":["framework_board"],"columnMapping":{"courseid":"Collection Id","collectionName":"Collection Name","batchid":"Batch Id","batchName":"Batch Name","userid":"User UUID","state":"State","district":"District","orgname":"Org Name","schooludisecode":"School Id","schoolname":"School Name","block":"Block Name","cluster":"Cluster Name","usertype":"User Type","usersubtype":"User Sub Type","framework_board":"Declared Board","enrolleddate":"Enrolment Date","completedon":"Completion Date","certificatestatus":"Certificate Status","completionPercentage":"Progress","total_sum_score":"Total Score"},"csvColumns":["courseid", "collectionName", "batchid", "batchName", "userid",  "state", "district", "orgname", "schooludisecode", "schoolname", "framework_board", "block", "cluster", "usertype", "usersubtype", "enrolleddate", "completedon", "certificatestatus", "completionPercentage"]},"parallelization":8,"appName":"Progress Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    ProgressExhaustJob.execute()

    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='progress-exhaust' and request_id='37564CF8F134EE7532F125651B51D17F-3'")

    while(pResponse.next()) {
      pResponse.getString("status") should be ("SUBMITTED")
      pResponse.getString("download_urls") should be ("{}")
      pResponse.getString("processed_batches") should not be (null)
    }

    val pResponse2 = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='progress-exhaust' and request_id='37564CF8F134EE7532F125651B51D17F-2'")
    val reportDate = getDate("yyyyMMdd").format(Calendar.getInstance().getTime)

    while(pResponse2.next()) {
      pResponse2.getString("status") should be ("SUCCESS")
      pResponse2.getString("download_urls") should be (s"{progress-exhaust/37564CF8F134EE7532F125651B51D17F-2/batch-001_progress_${reportDate}.zip}")
      pResponse2.getString("processed_batches") should not be (null)
    }
  }

  it should "test the exhaust reports with duplicate requests" in {

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130928636168192001667_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F-1', 'progress-exhaust', 'SUBMITTED', '{\"batchFilter\": [\"batch-004\", \"batch-003\"]}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130928636168192001667_batch-001:channel-02', '37564CF8F134EE7532F125651B51D17F-2', 'progress-exhaust', 'SUBMITTED', '{\"batchFilter\": [\"batch-004\", \"batch-003\"]}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.lms.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"localhost","sparkUserDbRedisPort":6341,"sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":"","userCacheCols":["framework_board"],"columnMapping":{"courseid":"Collection Id","collectionName":"Collection Name","batchid":"Batch Id","batchName":"Batch Name","userid":"User UUID","state":"State","district":"District","orgname":"Org Name","schooludisecode":"School Id","schoolname":"School Name","block":"Block Name","cluster":"Cluster Name","usertype":"User Type","usersubtype":"User Sub Type","framework_board":"Declared Board","enrolleddate":"Enrolment Date","completedon":"Completion Date","certificatestatus":"Certificate Status","completionPercentage":"Progress","total_sum_score":"Total Score"},"csvColumns":["courseid", "collectionName", "batchid", "batchName", "userid",  "state", "district", "orgname", "schooludisecode", "schoolname", "framework_board", "block", "cluster", "usertype", "usersubtype", "enrolleddate", "completedon", "certificatestatus", "completionPercentage"]},"parallelization":8,"appName":"Progress Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    ProgressExhaustJob.execute()

    val pResponse1 = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='progress-exhaust' and request_id='37564CF8F134EE7532F125651B51D17F-1'")

    while(pResponse1.next()) {
      pResponse1.getString("status") should be ("FAILED")
      pResponse1.getString("download_urls") should be ("{}")
      pResponse1.getString("processed_batches") should not be (null)

    }

    val pResponse2 = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='progress-exhaust' and request_id='37564CF8F134EE7532F125651B51D17F-2'")

    while(pResponse2.next()) {
      pResponse2.getString("status") should be ("FAILED")
      pResponse2.getString("download_urls") should be ("{}")
      pResponse2.getString("processed_batches") should not be (null)

    }
  }

  it should "provide hierarchy data on module level" in {
    implicit val sqlContext: SQLContext = spark.sqlContext
    import sqlContext.implicits._

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.lms.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"localhost","sparkUserDbRedisPort":6341,"sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":"","userCacheCols":["framework_board"],"columnMapping":{"courseid":"Collection Id","collectionName":"Collection Name","batchid":"Batch Id","batchName":"Batch Name","userid":"User UUID","state":"State","district":"District","orgname":"Org Name","schooludisecode":"School Id","schoolname":"School Name","block":"Block Name","cluster":"Cluster Name","usertype":"User Type","usersubtype":"User Sub Type","framework_board":"Declared Board","enrolleddate":"Enrolment Date","completedon":"Completion Date","certificatestatus":"Certificate Status","completionPercentage":"Progress","total_sum_score":"Total Score"},"csvColumns":["courseid", "collectionName", "batchid", "batchName", "userid",  "state", "district", "orgname", "schooludisecode", "schoolname", "framework_board", "block", "cluster", "usertype", "usersubtype", "enrolleddate", "completedon", "certificatestatus", "completionPercentage"]},"parallelization":8,"appName":"Progress Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    var collectionBatch = CollectionBatch("batch-001","do_1130928636168192001667","Basic Java","0130107621805015045","channel-01","channel-01","Test_TextBook_name_5197942513",Some("Yes"))
    var hierarchyData = List(ContentHierarchy("do_1130928636168192001667","""{"identifier":"do_1130928636168192001667","mimeType":"application/vnd.ekstep.content-collection","visibility":"Default","contentType":"Course","children":[{"parent":"do_112876961957437440179","identifier":"do_1130928636168192001667","lastStatusChangedOn":"2019-09-19T18:15:56.490+0000","code":"2cb4d698-dc19-4f0c-9990-96f49daff753","visibility":"Default","description":"Test_TextBookUnit_desc_8305852636","index":1,"mimeType":"application/vnd.ekstep.content-collection","createdOn":"2019-09-19T18:15:56.489+0000","versionKey":"1568916956489","depth":1,"name":"content_1","lastUpdatedOn":"2019-09-19T18:15:56.490+0000","contentType":"Course","children":[],"status":"Draft"}]}""")).toDF()

    var hierarchyModuleData = ProgressExhaustJob.getCollectionAggWithModuleData(collectionBatch, hierarchyData).collectAsList().asScala
    hierarchyModuleData.map(f => f.getString(0)) should contain theSameElementsAs  List("user-001", "user-002", "user-003", "user-004")
    hierarchyModuleData.map(f => f.getString(1)) should contain allElementsOf List("do_1130928636168192001667")
    hierarchyModuleData.map(f => f.getString(2)) should contain allElementsOf List("batch-001")
    hierarchyModuleData.map(f => f.getInt(3)) should contain allElementsOf List(100)
    hierarchyModuleData.map(f => f.getString(4)) should contain allElementsOf List("do_1130928636168192001667")
    hierarchyModuleData.map(f => f.getInt(5)) should contain allElementsOf List(100)

    // No mimetype, visibility etc available in hierarchy
    hierarchyData = List(ContentHierarchy("do_1130928636168192001667","""{"children":[{"parent":"do_112876961957437440179","identifier":"do_1130928636168192001667","lastStatusChangedOn":"2019-09-19T18:15:56.490+0000","code":"2cb4d698-dc19-4f0c-9990-96f49daff753","visibility":"Default","description":"Test_TextBookUnit_desc_8305852636","index":1,"mimeType":"application/vnd.ekstep.content-collection","createdOn":"2019-09-19T18:15:56.489+0000","versionKey":"1568916956489","depth":1,"name":"content_1","lastUpdatedOn":"2019-09-19T18:15:56.490+0000","contentType":"Course","status":"Draft"}]}""")).toDF()
    val hierarchyModuleData1 = ProgressExhaustJob.getCollectionAggWithModuleData(collectionBatch, hierarchyData).collectAsList().asScala

    hierarchyModuleData1.map(f => f.getString(0)) should contain theSameElementsAs  List("user-001", "user-002", "user-003", "user-004")
    hierarchyModuleData1.map(f => f.getString(1)) should contain allElementsOf List("do_1130928636168192001667")
    hierarchyModuleData1.map(f => f.getString(2)) should contain allElementsOf List("batch-001")
    hierarchyModuleData1.map(f => f.getInt(3)) should contain allElementsOf List(100)
    hierarchyModuleData1.map(f => f.getString(4)) should contain allElementsOf List(null)

    //levelCount is less than depthLevel
    var data =  List(Map("children" -> List(Map("lastStatusChangedOn" -> "2019-09-19T18:15:56.490+0000","parent" -> "do_112876961957437440179","children" -> List(),"name" -> "content_1","createdOn" -> "2019-09-19T18:15:56.489+0000"," lastUpdatedOn" -> "2019-09-19T18:15:56.490+0000", "identifier" -> "do_1130928636168192001667","description" -> "Test_TextBookUnit_desc_8305852636","versionKey" -> "1568916956489","mimeType" -> "application/vnd.ekstep.content-collection","code" -> "2cb4d698-dc19-4f0c-9990-96f49daff753"," contentType" -> "Course"," status" -> "Draft"," depth" -> "1"," visibility" -> "Default"," index" -> "1))"," identifier" -> "do_1130928636168192001667"," mimeType" -> "application/vnd.ekstep.content-collection"," contentType" -> "Course"," visibility" -> "Default"))))
    var prevData = CourseData("do_1130928636168192001667","0",List())
    var parseHierarchyData = ProgressExhaustJob.logTime(ProgressExhaustJob.parseCourseHierarchy(data, 3, prevData, 2), "Execution of ParseCourseHierarchy method")
    assert(parseHierarchyData.equals(prevData))

    //unit testcase for identifier and children not available
    collectionBatch = CollectionBatch("batch-001","do_1130928636168192001667","Basic Java","0130107621805015045","channel-01","channel-01","Test_TextBook_name_5197942513",Some("Yes"))
    hierarchyData = List(ContentHierarchy("do_1130928636168192001667","""{"mimeType":"application/vnd.ekstep.content-collection","visibility":"Default","contentType":"Course"}""")).toDF()

    hierarchyModuleData = ProgressExhaustJob.getCollectionAggWithModuleData(collectionBatch, hierarchyData).collectAsList().asScala
    hierarchyModuleData.map(f => f.getString(0)) should contain theSameElementsAs  List("user-001", "user-002", "user-003", "user-004")
    hierarchyModuleData.map(f => f.getString(1)) should contain allElementsOf List("do_1130928636168192001667")
    hierarchyModuleData.map(f => f.getString(2)) should contain allElementsOf List("batch-001")
    hierarchyModuleData.map(f => f.getInt(3)) should contain allElementsOf List(100)
    hierarchyModuleData.map(f => f.getString(4)) should contain allElementsOf List(null)
  }

  it should "validate the report path" in {
    val batch1 = "batch-001"
    val requestId = "37564CF8F134EE7532F125651B51D17F"
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.lms.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"localhost","sparkUserDbRedisPort":6341,"sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":"","userCacheCols":["framework_board"],"columnMapping":{"courseid":"Collection Id","collectionName":"Collection Name","batchid":"Batch Id","batchName":"Batch Name","userid":"User UUID","state":"State","district":"District","orgname":"Org Name","schooludisecode":"School Id","schoolname":"School Name","block":"Block Name","cluster":"Cluster Name","usertype":"User Type","usersubtype":"User Sub Type","framework_board":"Declared Board","enrolleddate":"Enrolment Date","completedon":"Completion Date","certificatestatus":"Certificate Status","completionPercentage":"Progress","total_sum_score":"Total Score"},"csvColumns":["courseid", "collectionName", "batchid", "batchName", "userid",  "state", "district", "orgname", "schooludisecode", "schoolname", "framework_board", "block", "cluster", "usertype", "usersubtype", "enrolleddate", "completedon", "certificatestatus", "completionPercentage"]},"parallelization":8,"appName":"Progress Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig
    val onDemandModeFilepath = ProgressExhaustJob.getFilePath(batch1, requestId)
    val reportDate = getDate("yyyyMMdd").format(Calendar.getInstance().getTime())
    onDemandModeFilepath should be(s"progress-exhaust/$requestId/batch-001_progress_$reportDate")

    val standAloneModeFilePath = ProgressExhaustJob.getFilePath(batch1, "")
    standAloneModeFilePath should be(s"progress-exhaust/batch-001_progress_$reportDate")
  }

  def getDate(pattern: String): SimpleDateFormat = {
    new SimpleDateFormat(pattern)
  }


//  it should "Generate a report for StandAlone Mode" in {
//   implicit val fc = new FrameworkContext()
//    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.lms.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"local","mode":"standalone","batchFilters":["TPD"],"searchFilter":{"request":{"filters":{"status":["Live"],"contentType":"Course"},"fields":["identifier","name","organisation","channel"],"limit":10}},"sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"localhost","sparkUserDbRedisPort":6341,"sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"Progress Exhaust"}"""
//    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
//    implicit val config = jobConfig
//    ProgressExhaustJob.execute()
//    val outputLocation = AppConf.getConfig("collection.exhaust.store.prefix")
//    val batch1 = "batch-001"
//    val filePath = ProgressExhaustJob.getFilePath(batch1, "")
//    implicit val responseExhaustEncoder = Encoders.product[ProgressExhaustReport]
//    val batch1Results = spark.read.format("csv").option("header", "true")
//      .load(s"$outputLocation/$filePath.csv").as[ProgressExhaustReport].collectAsList().asScala
//
//
//    batch1Results.size should be (4)
//    batch1Results.map(f => f.`Collection Id`).toList should contain atLeastOneElementOf List("do_1130928636168192001667")
//    batch1Results.map(f => f.`Collection Name`).toList should contain atLeastOneElementOf List("24 aug course")
//    batch1Results.map(f => f.`Batch Id`).toList should contain atLeastOneElementOf List("BatchId_batch-001")
//    batch1Results.map(f => f.`Batch Name`).toList should contain atLeastOneElementOf List("Basic Java")
//    batch1Results.map {res => res.`User UUID`}.toList should contain theSameElementsAs List("user-001", "user-002", "user-003", "user-004")
//    batch1Results.map {res => res.`State`}.toList should contain theSameElementsAs List("Karnataka", "Andhra Pradesh", "Karnataka", "Delhi")
//    batch1Results.map {res => res.`District`}.toList should contain theSameElementsAs List("bengaluru", "bengaluru", "bengaluru", "babarpur")
//    batch1Results.map(f => f.`Enrolment Date`).toList should contain allElementsOf  List("15/11/2019")
//    batch1Results.map(f => f.`Completion Date`).toList should contain allElementsOf  List(null)
//    batch1Results.map(f => f.`Progress`).toList should contain allElementsOf  List("100")
//    batch1Results.map(f => f.`Cluster Name`).toList should contain atLeastOneElementOf List("CLUSTER1")
//    batch1Results.map(f => f.`User Type`).toList should contain atLeastOneElementOf List("administrator")
//    batch1Results.map(f => f.`User Sub Type`).toList should contain atLeastOneElementOf List("deo")
//  }

  /*
   * Testcase for getting the latest value from migrated date fields
   * enrolleddate: (Old Field)
   *   2019-11-13 05:41:50:382+0000
   *   null
   *   2019-11-15 05:41:50:382+0000
   * enrolled_date: (New Field)
   *   2019-11-16 05:41:50
   *   2019-11-15 05:41:50
   *   null
   * expected result enrolleddate:
   *   16/11/2019
   *   15/11/2019
   *   15/11/2019
   */
  it should "generate the report with the latest value from date columns" in {

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130928636168192001667_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'progress-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-001\"}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.lms.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"localhost","sparkUserDbRedisPort":6341,"sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":"","userCacheCols":["framework_board"],"columnMapping":{"courseid":"Collection Id","collectionName":"Collection Name","batchid":"Batch Id","batchName":"Batch Name","userid":"User UUID","state":"State","district":"District","orgname":"Org Name","schooludisecode":"School Id","schoolname":"School Name","block":"Block Name","cluster":"Cluster Name","usertype":"User Type","usersubtype":"User Sub Type","framework_board":"Declared Board","enrolleddate":"Enrolment Date","completedon":"Completion Date","certificatestatus":"Certificate Status","completionPercentage":"Progress","total_sum_score":"Total Score"},"csvColumns":["courseid", "collectionName", "batchid", "batchName", "userid",  "state", "district", "orgname", "schooludisecode", "schoolname", "framework_board", "block", "cluster", "usertype", "usersubtype", "enrolleddate", "completedon", "certificatestatus", "completionPercentage"]},"parallelization":8,"appName":"Progress Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    ProgressExhaustJob.execute()

    val outputLocation = AppConf.getConfig("collection.exhaust.store.prefix")
    val outputDir = "progress-exhaust"
    val batch1 = "batch-001"
    val requestId = "37564CF8F134EE7532F125651B51D17F"
    val filePath = ProgressExhaustJob.getFilePath(batch1, requestId)
    val jobName = ProgressExhaustJob.jobName()

    implicit val responseExhaustEncoder = Encoders.product[ProgressExhaustReport]
    val batch1Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$filePath.csv").select("User UUID", "Enrolment Date")

    batch1Results.count should be (4)
    batch1Results.filter(col("User UUID") === "user-001").collect().map(_ (1)).toList(0) should be("16/11/2019")
    batch1Results.filter(col("User UUID") === "user-002").collect().map(_ (1)).toList(0) should be("15/11/2019")
    batch1Results.filter(col("User UUID") === "user-003").collect().map(_ (1)).toList(0) should be("15/11/2019")
  }

  it should "generate report validating and filtering duplicate batches" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130928636168192001667_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'progress-exhaust', 'SUBMITTED', '{\"batchFilter\": [\"batch-01\", \"batch-001\", \"batch-001\"]}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.lms.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"localhost","sparkUserDbRedisPort":6341,"sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":"","userCacheCols":["framework_board"],"columnMapping":{"courseid":"Collection Id","collectionName":"Collection Name","batchid":"Batch Id","batchName":"Batch Name","userid":"User UUID","state":"State","district":"District","orgname":"Org Name","schooludisecode":"School Id","schoolname":"School Name","block":"Block Name","cluster":"Cluster Name","usertype":"User Type","usersubtype":"User Sub Type","framework_board":"Declared Board","enrolleddate":"Enrolment Date","completedon":"Completion Date","certificatestatus":"Certificate Status","completionPercentage":"Progress","total_sum_score":"Total Score"},"csvColumns":["courseid", "collectionName", "batchid", "batchName", "userid",  "state", "district", "orgname", "schooludisecode", "schoolname", "framework_board", "block", "cluster", "usertype", "usersubtype", "enrolleddate", "completedon", "certificatestatus", "completionPercentage"]},"parallelization":8,"appName":"Progress Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    ProgressExhaustJob.execute()

    val outputLocation = AppConf.getConfig("collection.exhaust.store.prefix")
    val outputDir = "progress-exhaust"
    val batch1 = "batch-001"
    val requestId = "37564CF8F134EE7532F125651B51D17F"
    val filePath = ProgressExhaustJob.getFilePath(batch1, requestId)
    val jobName = ProgressExhaustJob.jobName()

    implicit val responseExhaustEncoder = Encoders.product[ProgressExhaustReport]
    val batch1Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$filePath.csv").as[ProgressExhaustReport].collectAsList().asScala

    batch1Results.size should be (4)
    batch1Results.map(f => f.`Collection Id`).toList should contain atLeastOneElementOf List("do_1130928636168192001667")
    batch1Results.map(f => f.`Collection Name`).toList should contain atLeastOneElementOf List("24 aug course")
    batch1Results.map(f => f.`Batch Id`).toList should contain atLeastOneElementOf List("BatchId_batch-001")
    batch1Results.map(f => f.`Batch Name`).toList should contain atLeastOneElementOf List("Basic Java")
    batch1Results.map {res => res.`User UUID`}.toList should contain theSameElementsAs List("user-001", "user-002", "user-003", "user-004")
    batch1Results.map {res => res.`State`}.toList should contain theSameElementsAs List("Karnataka", "Andhra Pradesh", "Karnataka", "Delhi")
    batch1Results.map {res => res.`District`}.toList should contain theSameElementsAs List("bengaluru", "bengaluru", "bengaluru", "babarpur")
    batch1Results.map(f => f.`Enrolment Date`).toList should contain allElementsOf  List("15/11/2019")
    batch1Results.map(f => f.`Completion Date`).toList should contain allElementsOf  List(null)
    batch1Results.map(f => f.`Progress`).toList should contain allElementsOf  List("100")
    batch1Results.map(f => f.`Cluster Name`).toList should contain atLeastOneElementOf List("CLUSTER1")
    batch1Results.map(f => f.`User Type`).toList should contain atLeastOneElementOf List("administrator")
    batch1Results.map(f => f.`User Sub Type`).toList should contain atLeastOneElementOf List("deo")

    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='progress-exhaust'")
    val reportDate = getDate("yyyyMMdd").format(Calendar.getInstance().getTime())

    while(pResponse.next()) {
      pResponse.getString("status") should be ("SUCCESS")
      pResponse.getString("err_message") should be ("")
      pResponse.getString("dt_job_submitted") should be ("2020-10-19 05:58:18.666")
      pResponse.getString("download_urls") should be (s"""{progress-exhaust/$requestId/batch-001_progress_${reportDate}.zip}""")
      pResponse.getString("dt_file_created") should be (null)
      pResponse.getString("iteration") should be ("0")
    }

    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, outputLocation)
  }

  it should "mark request as failed if all batches are invalid in request_data" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130928636168192001667_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'progress-exhaust', 'SUBMITTED', '{\"batchFilter\": [\"batch-01\", \"batch-02\"]}',  'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.lms.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"localhost","sparkUserDbRedisPort":6341,"sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":"","userCacheCols":["framework_board"],"columnMapping":{"courseid":"Collection Id","collectionName":"Collection Name","batchid":"Batch Id","batchName":"Batch Name","userid":"User UUID","state":"State","district":"District","orgname":"Org Name","schooludisecode":"School Id","schoolname":"School Name","block":"Block Name","cluster":"Cluster Name","usertype":"User Type","usersubtype":"User Sub Type","framework_board":"Declared Board","enrolleddate":"Enrolment Date","completedon":"Completion Date","certificatestatus":"Certificate Status","completionPercentage":"Progress","total_sum_score":"Total Score"},"csvColumns":["courseid", "collectionName", "batchid", "batchName", "userid",  "state", "district", "orgname", "schooludisecode", "schoolname", "framework_board", "block", "cluster", "usertype", "usersubtype", "enrolleddate", "completedon", "certificatestatus", "completionPercentage"]},"parallelization":8,"appName":"Progress Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    ProgressExhaustJob.execute()

    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='progress-exhaust'")
    val reportDate = getDate("yyyyMMdd").format(Calendar.getInstance().getTime())

    while(pResponse.next()) {
      pResponse.getString("status") should be ("FAILED")
      pResponse.getString("request_data") should be ("""{"batchFilter": ["batch-01", "batch-02"]}""")
      pResponse.getString("err_message") should be ("No data found")
      pResponse.getString("dt_job_submitted") should be ("2020-10-19 05:58:18.666")
      pResponse.getString("download_urls") should be (s"""{}""")
      pResponse.getString("dt_file_created") should be (null)
      pResponse.getString("iteration") should be ("1")
    }

  }

//  it should "insert status as FAILED since course is retired" in {
//
//    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
//    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130928636168192001667_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'progress-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-005\"}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")
//
//    implicit val fc = new FrameworkContext()
//    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.lms.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"localhost","sparkUserDbRedisPort":6341,"sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"Progress Exhaust"}"""
//    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
//    implicit val config = jobConfig
//
//    ProgressExhaustJob.execute()
//
//    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='progress-exhaust'")
//    val reportDate = getDate("yyyyMMdd").format(Calendar.getInstance().getTime())
//
//    while(pResponse.next()) {
//      pResponse.getString("status") should be ("FAILED")
//      pResponse.getString("err_message") should be ("The request is made for retired collection")
//      pResponse.getString("download_urls") should be (s"""{}""")
//    }
//  }
}
