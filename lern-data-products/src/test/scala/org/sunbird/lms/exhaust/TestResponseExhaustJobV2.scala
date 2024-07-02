package org.sunbird.lms.exhaust

import okhttp3.mockwebserver.{Dispatcher, MockResponse, MockWebServer, RecordedRequest}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.joda.time.DateTimeZone
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.scalamock.scalatest.MockFactory
import org.sunbird.core.exhaust.BaseReportsJob
import org.sunbird.core.util.{BaseSpec, EmbeddedCassandra, EmbeddedPostgresql, RedisConnect}
import org.sunbird.lms.exhaust.collection.ResponseExhaustJobV2
import redis.clients.jedis.Jedis
import redis.embedded.RedisServer

import scala.collection.JavaConverters._

class TestResponseExhaustJobV2 extends BaseSpec with MockFactory with BaseReportsJob {

  val esServer = new MockWebServer()

  val esDispatcher: Dispatcher = new Dispatcher() {
    @throws[InterruptedException]
    override def dispatch(request: RecordedRequest): MockResponse = {
      (request.getPath, request.getMethod) match {
        case ("/api/content/v1/search", "POST") =>
          new MockResponse().setHeader("Content-Type", "application/json").setResponseCode(200).setBody("""{"id":"api.content.search","ver":"1.0","ts":"2023-02-16T07:19:30.405Z","params":{"resmsgid":"4102b950-adca-11ed-90b2-b7bf811d5c69","msgid":"40fbdb80-adca-11ed-a723-4b0cc0e91be5","status":"successful","err":null,"errmsg":null},"responseCode":"OK","result":{"count":712392,"content":[{"ownershipType":["createdBy"],"publish_type":"public","unitIdentifiers":["do_21337599437489766414999"],"copyright":"Test axis,2126","se_gradeLevelIds":["sunbird_gradelevel_class10"],"previewUrl":"https://obj.stage.sunbirded.org/sunbird-content-staging/content/do_21337600715072307215013/artifact/do_21337600715072307215013_1632814051137_do_21337600715072307215013_1632813407308_pdf_229.pdf","organisationId":"c5b2b2b9-fafa-4909-8d5d-f9458d1a3881","keywords":["All_Contents"],"subject":["Hindi"],"downloadUrl":"https://obj.stage.sunbirded.org/sunbird-content-staging/content/do_21337600715072307215013/learning-resource_1671091450835_do_21337600715072307215013_2.ecar","channel":"b00bc992ef25f1a9a8d63291e20efc8d","language":["English"],"variants":{"full":{"ecarUrl":"https://obj.stage.sunbirded.org/sunbird-content-staging/content/do_21337600715072307215013/learning-resource_1671091450835_do_21337600715072307215013_2.ecar","size":"262229"},"spine":{"ecarUrl":"https://obj.stage.sunbirded.org/sunbird-content-staging/content/do_21337600715072307215013/learning-resource_1671091451013_do_21337600715072307215013_2_SPINE.ecar","size":"7647"}},"source":"https://dockstaging.sunbirded.org/api/content/v1/read/do_21337600715072307215013","mimeType":"application/pdf","objectType":"Content","se_mediums":["English"],"appIcon":"https://stagingdock.blob.core.windows.net/sunbird-content-dock/content/do_21337600715072307215013/artifact/apple-fruit.thumb.jpg","gradeLevel":["Class 10"],"primaryCategory":"Learning Resource","appId":"staging.dock.portal","artifactUrl":"https://obj.stage.sunbirded.org/sunbird-content-staging/content/do_21337600715072307215013/artifact/do_21337600715072307215013_1632814051137_do_21337600715072307215013_1632813407308_pdf_229.pdf","contentEncoding":"identity","contentType":"PreviousBoardExamPapers","se_gradeLevels":["Class 10"],"trackable":{"enabled":"No","autoBatch":"No"},"identifier":"do_1130928636168192001667","se_boardIds":["sunbird_testboard"],"subjectIds":["sunbird_subject_hindi"],"audience":["Student"],"visibility":"Default","consumerId":"cb069f8d-e4e1-46c5-831f-d4a83b323ada","author":"paul1","discussionForum":{"enabled":"No"},"mediaType":"content","osId":"org.ekstep.quiz.app","lastPublishedBy":"9c9b3259-f137-491a-bae5-fe2ad1763647","languageCode":["en"],"graph_id":"domain","nodeType":"DATA_NODE","version":2,"pragma":["external"],"se_subjects":["Hindi"],"prevState":"Review","license":"CC BY 4.0","lastPublishedOn":"2019-11-15 05:41:50:382+0000","size":270173,"name":"SelfAssess for course","topic":["कर चले हम फ़िदा"],"mediumIds":["sunbird_medium_english"],"attributions":[""],"status":"Live","topicsIds":["sunbird_topic_62113a9a1815b8f14e3103458f2b7c56cf2eeaf5"],"interceptionPoints":"{}","code":"05747b0c-cec4-61fd-52b6-1c4239c662ae","credentials":{"enabled":"No"},"prevStatus":"Draft","origin":"do_21337600715072307215013","description":"\"This is nadiya\"*","streamingUrl":"https://obj.stage.sunbirded.org/sunbird-content-staging/content/do_21337600715072307215013/artifact/do_21337600715072307215013_1632814051137_do_21337600715072307215013_1632813407308_pdf_229.pdf","medium":["English"],"posterImage":"https://stagingdock.blob.core.windows.net/sunbird-content-dock/content/do_21337600715072307215013/artifact/apple-fruit.jpg","idealScreenSize":"normal","createdOn":"2019-11-15 05:41:50:382+0000","se_boards":["TestBoard"],"se_mediumIds":["sunbird_medium_english"],"processId":"0c7a375e-fe42-4836-8e2d-e901d03a600e","contentDisposition":"inline","lastUpdatedOn":"2019-11-15 05:41:50:382+0000","originData":"{\"identifier\":\"do_21337600715072307215013\",\"repository\":\"https://dockstaging.sunbirded.org/api/content/v1/read/do_21337600715072307215013\"}","se_topicIds":["sunbird_topic_62113a9a1815b8f14e3103458f2b7c56cf2eeaf5"],"collectionId":"batch-001","dialcodeRequired":"No","createdFor":["01329314824202649627"],"creator":"paul1@yopmail.com","lastStatusChangedOn":"2019-11-15 05:41:50:382+0000","os":["All"],"cloudStorageKey":"content/do_21337600715072307215013/artifact/do_21337600715072307215013_1632814051137_do_21337600715072307215013_1632813407308_pdf_229.pdf","se_subjectIds":["sunbird_subject_hindi"],"se_FWIds":["sunbird"],"targetFWIds":["sunbird"],"pkgVersion":2,"versionKey":"1632814052871","migrationVersion":1.1,"idealScreenDensity":"hdpi","s3Key":"content/do_21337600715072307215013/artifact/do_21337600715072307215013_1632814051137_do_21337600715072307215013_1632813407308_pdf_229.pdf","boardIds":["sunbird_testboard"],"framework":"sunbird","lastSubmittedOn":"2019-11-15 05:41:50:382+0000","createdBy":"5b39b2a0-e9ba-4bab-9b05-91ad2e28b626","se_topics":["कर चले हम फ़िदा"],"compatibilityLevel":4,"gradeLevelIds":["sunbird_gradelevel_class10"],"programId":"b9b8eb80-2027-11ec-bd97-0d10d9b33e3f","board":"TestBoard","node_id":1116608}]}}""")
      }
    }
  }

  val jobRequestTable = "job_request"
  implicit var spark: SparkSession = _
  var redisServer: RedisServer = _
  var jedis: Jedis = _
  val outputLocation = AppConf.getConfig("collection.exhaust.store.prefix")
  val tenantPrefWebserver = new MockWebServer()

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();
    esServer.setDispatcher(esDispatcher)
    esServer.start(9080)
    redisServer = new RedisServer(6341)
    redisServer.start()

    val tenantPrefDispatcher: Dispatcher = new Dispatcher() {
      @throws[InterruptedException]
      override def dispatch(request: RecordedRequest): MockResponse = {
        val body = new String(request.getBody.readByteArray())
        val jsonBody = JSONUtils.deserialize[Map[String, AnyRef]](body)
        val requestBody = jsonBody.getOrElse("request", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].getOrElse("key", "")
        (request.getPath, request.getMethod, requestBody) match {
          case ("/private/v2/org/preferences/read", "POST", "dataSecurityPolicy") =>
            new MockResponse().setHeader("Content-Type", "application/json").setResponseCode(200).setBody("""{"id":".private.v2.org.preferences.read","ver":"private","ts":"2023-05-26 16:25:42:913+0000","params":{"resmsgid":"976058ce-570a-4c56-a5f9-623141bedd4a","msgid":"976058ce-570a-4c56-a5f9-623141bedd4a","err":null,"status":"SUCCESS","errmsg":null},"responseCode":"OK","result":{"response":{"updatedBy":"fbe926ac-a395-40e4-a65b-9b4f711d7642","data":{"level":"PLAIN_DATASET","dataEncrypted":"No","comments":"Data is not encrypted","job":{"progress-exhaust":{"level":"PUBLIC_KEY_ENCRYPTED_DATASET","dataEncrypted":"No","comments":"Password protected file."},"response-exhaust":{"level":"TEXT_KEY_ENCRYPTED_DATASET","dataEncrypted":"No","comments":"Password protected file."},"userinfo-exhaust":{"level":"PASSWORD_PROTECTED_DATASET","dataEncrypted":"No","comments":"Password protected file."},"program-user-exhaust":{"level":"TEXT_KEY_ENCRYPTED_DATASET","dataEncrypted":"Yes","comments":"Text key Encrypted File"}},"securityLevels":{"PLAIN_DATASET":"Data is present in plain text/zip. Generally applicable to open datasets.","PASSWORD_PROTECTED_DATASET":"Password protected zip file. Generally applicable to non PII data sets but can contain sensitive information which may not be considered open.","TEXT_KEY_ENCRYPTED_DATASET":"Data encrypted with a user provided encryption key. Generally applicable to non PII data but can contain sensitive information which may not be considered open.","PUBLIC_KEY_ENCRYPTED_DATASET":"Data encrypted via an org provided public/private key. Generally applicable to all PII data exhaust."}},"createdBy":"fbe926ac-a395-40e4-a65b-9b4f711d7642","updatedOn":1684825029544,"createdOn":1682501851315,"orgId":"default","key":"dataSecurityPolicy"}}}""")
          case ("/private/v2/org/preferences/read", "POST", "userPrivateFields") =>
            new MockResponse().setHeader("Content-Type", "application/json").setResponseCode(200).setBody("""{"id":".private.v2.org.preferences.read","ver":"private","ts":"2023-05-26 16:25:42:913+0000","params":{"resmsgid":"976058ce-570a-4c56-a5f9-623141bedd4a","msgid":"976058ce-570a-4c56-a5f9-623141bedd4a","err":null,"status":"SUCCESS","errmsg":null},"responseCode":"OK","result":{"response":{"data": {"piiFields":["courseid", "collectionName", "batchid", "batchName", "userid", "content_id", "contentname", "attempt_id", "last_attempted_on", "questionid", "questiontype", "questiontitle", "questiondescription", "questionduration", "questionscore", "questionmaxscore", "questionoption", "questionresponse"]}}}}""")
        }
      }
    }

    tenantPrefWebserver.setDispatcher(tenantPrefDispatcher)
    tenantPrefWebserver.start(9090)

    setupRedisData()
    EmbeddedCassandra.loadData("src/test/resources/exhaust/report_data.cql") // Load test data in embedded cassandra server
    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createJobRequestTable()
  }

  override def afterAll() : Unit = {
    super.afterAll()
    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, outputLocation)
    redisServer.stop()
    spark.close()
    esServer.close()
    tenantPrefWebserver.close()
    EmbeddedCassandra.close()
    EmbeddedPostgresql.close()
  }

  def setupRedisData(): Unit = {
    val redisConnect = new RedisConnect("localhost", 6341)
    val jedis = redisConnect.getConnection(0, 100000)
    jedis.hmset("user:user-001", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Manju", "userid": "user-001", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "manju@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-002", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Mahesh", "userid": "user-002", "state": "Andhra Pradesh", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "01285019302823526477", "email": "mahesh@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-003", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Sowmya", "userid": "user-003", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "sowmya@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-004", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Utkarsha", "userid": "user-004", "state": "Delhi", "district": "babarpur", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "utkarsha@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-005", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Isha", "userid": "user-005", "state": "MP", "district": "Jhansi", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "isha@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-006", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Revathi", "userid": "user-006", "state": "Andhra Pradesh", "district": "babarpur", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "revathi@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-007", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Sunil", "userid": "user-007", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0126391644091351040", "email": "sunil@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-008", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Anoop", "userid": "user-008", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "anoop@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-009", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Kartheek", "userid": "user-009", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "01285019302823526477", "email": "kartheekp@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-010", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Anand", "userid": "user-010", "state": "Tamil Nadu", "district": "Chennai", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "anandp@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.close()
  }

  "TestResponseExhaustJobV2" should "generate final output as csv and zip files" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration,encryption_key) VALUES ('do_1131350140968632321230_batch-001:01250894314817126443', '37564CF8F134EE7532F125651B51D17F', 'response-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-001\"}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0,'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.lms.exhaust.collection.ResponseExhaustJobV2","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"assessmentFetcherConfig":{"store":"local","filePath":"src/test/resources/exhaust/data-archival/"},"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"0","sparkUserDbRedisPort":6341,"sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":"","columnMapping":{"courseid":"Collection Id","collectionName":"Collection Name","batchid":"Batch Id","batchName":"Batch Name","userid":"User UUID","content_id":"QuestionSet Id","contentname":"QuestionSet Title","attempt_id":"Attempt Id","last_attempted_on":"Attempted On","questionid":"Question Id","questiontype":"Question Type","questiontitle":"Question Title","questiondescription":"Question Description","questionduration":"Question Duration","questionscore":"Question Score","questionmaxscore":"Question Max Score","questionoption":"Question Options","questionresponse":"Question Response"},"csvColumns":["courseid", "collectionName", "batchid", "batchName", "userid", "content_id", "contentname", "attempt_id", "last_attempted_on", "questionid", "questiontype", "questiontitle", "questiondescription", "questionduration", "questionscore", "questionmaxscore", "questionoption", "questionresponse"]},"parallelization":8,"appName":"ResponseExhaustJob Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    ResponseExhaustJobV2.execute()

    val outputDir = "response-exhaust"
    val batch1 = "batch-001"
    val requestId = "37564CF8F134EE7532F125651B51D17F"
    val filePath = ResponseExhaustJobV2.getFilePath(batch1, requestId)
    val jobName = ResponseExhaustJobV2.jobName()

    implicit val responseExhaustEncoder = Encoders.product[ResponseExhaustReport]
    val batch1Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$filePath.csv").as[ResponseExhaustReport].collectAsList().asScala
    batch1Results.size should be (16)

    val user1Result = batch1Results.filter(f => f.`User UUID`.equals("user-001"))
    user1Result.map(f => f.`Collection Id`).toList should contain atLeastOneElementOf List("do_1130928636168192001667")
    user1Result.map(f => f.`Batch Id`).toList should contain atLeastOneElementOf List("BatchId_batch-001")
    user1Result.map(f => f.`User UUID`).toList should contain atLeastOneElementOf List("user-001")
    user1Result.map(f => f.`Attempt Id`).toList should contain atLeastOneElementOf List("attempat-001")
    user1Result.map(f => f.`QuestionSet Id`).toList should contain atLeastOneElementOf List("do_1128870328040161281204", "do_112876961957437440179")
    user1Result.map(f => f.`QuestionSet Title`).toList should contain atLeastOneElementOf List("SelfAssess for course", "Assessment score report using summary plugin",null)
    user1Result.map(f => f.`Question Id`).toList should contain theSameElementsAs List("do_213019475454476288155", "do_213019970118279168165", "do_213019972814823424168", "do_2130256513760624641171")
    user1Result.map(f => f.`Question Type`).toList should contain theSameElementsAs List("mcq", "mcq", "mtf", "mcq")
    user1Result.map(f => f.`Question Title`).toList should contain atLeastOneElementOf List("testQuestiontextandformula", "test with formula")
    user1Result.map(f => f.`Question Description`).toList should contain atLeastOneElementOf  List("testQuestiontextandformula")
    user1Result.map(f => f.`Question Duration`).toList should contain theSameElementsAs List("1.0", "1.0", "2.0", "12.0")
    user1Result.map(f => f.`Question Score`).toList should contain theSameElementsAs List("1.0", "1.0", "0.33", "10.0")
    user1Result.map(f => f.`Question Max Score`).toList should contain theSameElementsAs List("1.0", "1.0", "1.0", "10.0")
    user1Result.map(f => f.`Question Options`).head should be ("""[{'1':'{'text':'A=pi r^2'}'},{'2':'{'text':'no'}'},{'answer':'{'correct':['1']}'}]""")
    user1Result.map(f => f.`Question Response`).head should be ("""[{'1':'{'text':'A=pi r^2'}'}]""")

    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='response-exhaust'")

    while(pResponse.next()) {
      pResponse.getString("err_message") should be ("")
      pResponse.getString("dt_job_submitted") should be ("2020-10-19 05:58:18.666")
      pResponse.getString("download_urls") should be (s"{response-exhaust/$requestId/batch-001_response_${getDate()}.zip}")
      pResponse.getString("dt_file_created") should be (null)
      pResponse.getString("iteration") should be ("0")
    }

  }

  "TestResponseExhaustJobV2" should "generate report even if blob does not has any data for the batchid" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration) VALUES ('do_1131350140968632321230_batch-001:01250894314817126443', '37564CF8F134EE7532F125651B51D17F', 'response-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-001\"}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0);")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.lms.exhaust.collection.ResponseExhaustJobV2","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"assessmentFetcherConfig":{"store":"local","filePath":"src/test/resources/exhaust/data-archival/blob-data/","format":"csv"},"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"0","sparkUserDbRedisPort":6341,"sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"ResponseExhaustJob Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    ResponseExhaustJobV2.execute()
  }

  def getDate(): String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    dateFormat.print(System.currentTimeMillis());
  }

}