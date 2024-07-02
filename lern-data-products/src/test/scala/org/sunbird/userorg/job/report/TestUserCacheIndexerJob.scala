package org.sunbird.userorg.job.report

import okhttp3.mockwebserver.{Dispatcher, MockResponse, MockWebServer, RecordedRequest}
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.util.JSONUtils
import org.scalamock.scalatest.MockFactory
import org.sunbird.core.util.{EmbeddedCassandra, RedisConnect, SparkSpec}
import redis.clients.jedis.Jedis
import redis.embedded.RedisServer

class TestUserCacheIndexerJob extends SparkSpec(null) with MockFactory {
  implicit var spark: SparkSession = _
  var redisServer: RedisServer = _
  val fwServer = new MockWebServer()

  val fwDispatcher: Dispatcher = new Dispatcher() {
    @throws[InterruptedException]
    override def dispatch(request: RecordedRequest): MockResponse = {
      (request.getPath, request.getMethod) match {
        case ("/api/framework/v1/read/NCF", "GET") =>
          new MockResponse().setHeader("Content-Type", "application/json").setResponseCode(200).setBody("""{"id":"api.framework.read","ver":"1.0","ts":"2023-11-21T06:45:06.913Z","params":{"resmsgid":"81e95510-8839-11ee-9f02-c1254341007b","msgid":"81e7ce70-8839-11ee-abb8-9b56d1225ae8","status":"successful","err":null,"errmsg":null},"responseCode":"OK","result":{"framework":{"identifier":"NCF","code":"NCF","name":"NCF","description":"Sunbird K-12 framework","categories":[{"identifier":"ncf_board","code":"board","terms":[],"translations":null,"name":"Board","description":null,"index":1,"status":"Live"},{"identifier":"ncf_medium","code":"medium","terms":[],"translations":null,"name":"Medium","description":null,"index":2,"status":"Live"},{"identifier":"ncf_gradelevel","code":"gradeLevel","terms":[],"translations":null,"name":"Grade Level","description":null,"index":3,"status":"Live"},{"identifier":"ncf_subject","code":"subject","terms":[],"translations":null,"name":"Subject","description":null,"index":4,"status":"Live"}],"type":"K-12","objectType":"Framework"}}}""")
        case ("/api/framework/v1/read/agri", "GET") =>
          new MockResponse().setHeader("Content-Type", "application/json").setResponseCode(200).setBody("""{"id":"api.framework.read","ver":"1.0","ts":"2023-11-21T06:44:54.682Z","params":{"resmsgid":"7a9f07a0-8839-11ee-9f02-c1254341007b","msgid":"7a9dcf20-8839-11ee-abb8-9b56d1225ae8","status":"successful","err":null,"errmsg":null},"responseCode":"OK","result":{"framework":{"identifier":"agriculture_framework","code":"agriculture_framework","name":"agriculture_framework","description":"Sunbird TPD framework","categories":[{"identifier":"agriculture_framework_foodcrops","code":"foodcrops","terms":[],"translations":null,"name":"foodcrops","description":null,"index":1,"status":"Live"},{"identifier":"agriculture_framework_commercialcrops","code":"commercialcrops","terms":[],"translations":null,"name":"commercialcrops","description":null,"index":2,"status":"Live"},{"identifier":"agriculture_framework_livestockmanagement","code":"livestockmanagement","terms":[],"translations":null,"name":"livestockmanagement","description":null,"index":3,"status":"Live"},{"identifier":"agriculture_framework_livestockspecies","code":"livestockspecies","terms":[],"translations":null,"name":"livestockspecies","description":null,"index":4,"status":"Live"},{"identifier":"agriculture_framework_animalwelfare","code":"animalwelfare","terms":[],"translations":null,"name":"animalwelfare","description":null,"index":5,"status":"Live"}],"type":"K-12","objectType":"Framework"}}}""")
      }
    }
  }

  override def beforeAll(): Unit = {
    spark = getSparkSession();
    super.beforeAll()
    fwServer.setDispatcher(fwDispatcher)
    fwServer.start(9100)
    redisServer = new RedisServer(6341)
    redisServer.start()
    setupRedisData()
    EmbeddedCassandra.loadData("src/test/resources/exhaust/report_data.cql") // Load test data in embedded cassandra server
  }

  override def afterAll(): Unit = {
    super.afterAll()
    redisServer.stop()
    println("******** closing the redis connection **********" + redisServer.isActive)
    EmbeddedCassandra.close()
    spark.close()
  }

  def setupRedisData(): Unit = {
    val redisConnect = new RedisConnect("localhost", 6341)
    val jedis = redisConnect.getConnection(0, 100000)
    jedis.hmset("user:user-001", JSONUtils.deserialize[java.util.Map[String, String]]("""{"cluster":"CLUSTER1","firstname":"Manju","subject":"[\"IRCS\"]","schooludisecode":"3183211","usertype":"administrator","usersignintype":"Validated","language":"[\"English\"]","medium":"[\"English\"]","userid":"a962a4ff-b5b5-46ad-a9fa-f54edf1bcccb","schoolname":"DPS, MATHURA","rootorgid":"01250894314817126443","lastname":"Kapoor","framework":"[\"sunbird_health\"]","orgname":"Root Org2","phone":"","usersubtype":"deo","district":"bengaluru","grade":"[\"Volunteers\"]","block":"BLOCK1","state":"Karnataka","board":"[\"Sunbird-Health\"]","email":""};"""))
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

  def getRedisConnection: Jedis = {
    val redisConnect = new RedisConnect("localhost", 6341)
    redisConnect.getConnection(0, 100000)
  }

//  TODO: Enable the testcase
  ignore should "generate the report with all the correct data" in {
    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.userorg.job.report.UserCacheIndexerJob","modelParams":{"specificUserId":"","fromSpecificDate":"","populateAnonymousData":"false","refreshUserData":"true","sparkRedisConnectionHost":"localhost","sparkUserDbRedisPort":"6341"}}"""
    val metrics: Unit = UserCacheIndexerJob.main(strConfig)
    val Jedis = getRedisConnection
    val userData = Jedis.hgetAll("user:56c2d9a3-fae9-4341-9862-4eeeead2e9a1")
    assert(userData.size()===14)
    userData.get("firstname") should be("localuser118f")
  }
}
