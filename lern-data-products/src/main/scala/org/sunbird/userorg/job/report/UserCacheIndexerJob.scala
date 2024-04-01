package org.sunbird.userorg.job.report

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, collect_set, concat_ws, explode_outer, lit, lower, to_json, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig}
import org.ekstep.analytics.framework.Level.INFO
import org.sunbird.lms.exhaust.collection.CollectionDetails
import redis.clients.jedis.Jedis

import scala.collection.mutable

case class LocationId(userid: String, locationids: List[String])

case class ProfileUserType(userid: String, usertype: String, usersubtype: String, profileusertypes: String)

object UserCacheIndexerJob extends IJob with Serializable {

  implicit val className: String = "org.sunbird.userorg.job.report.UserCacheIndexerJob"

  def name(): String = "UserCacheIndexerJob"

  def main(jobDataConfig: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None): Unit = {
    val config: Config = ConfigFactory.load
    JobLogger.init(name())
    JobLogger.start("Started executing", Option(Map("config" -> config, "model" -> name)))
    val jobConfig = JSONUtils.deserialize[JobConfig](jobDataConfig)
    val specificUserId: String = if(jobConfig.modelParams.get.getOrElse("specificUserId", "null").toString != "null") jobConfig.modelParams.get("specificUserId").toString else null
    val fromSpecificDate: String = if(jobConfig.modelParams.get.getOrElse("fromSpecificDate", "null").toString != "null") jobConfig.modelParams.get("fromSpecificDate").toString else null
    val populateAnonymousData: String = jobConfig.modelParams.get.getOrElse("populateAnonymousData", "false").toString // populate anonymous data
    val refreshUserData: String = jobConfig.modelParams.get.getOrElse("refreshUserData", "false").toString // refresh existing user data
    JobLogger.log(s"params for the job: specificUserId: ${specificUserId} ::: fromSpecificDate: ${fromSpecificDate} ::: populateAnonymousData: ${populateAnonymousData} ::: refreshUserData: ${refreshUserData}", None, INFO)
    val sunbirdKeyspace = "sunbird"
    val complexFieldTypes = Array("array", "map")
    val fwReadApiUrl = config.getString("taxonomy.basePath") + config.getString("framework_read_api")
    val redisIndex = if (!populateAnonymousData.equalsIgnoreCase("true")) config.getString("redis.user.database.index") else config.getString("redis.user.input.index")
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("AppName")
        .config("spark.master", "local[*]")
        .config("spark.cassandra.connection.host", config.getString("sunbird.user.cluster.host"))
        .config("spark.redis.host", jobConfig.modelParams.get.getOrElse("sparkRedisConnectionHost", "").toString)
        .config("spark.redis.port", jobConfig.modelParams.get.getOrElse("sparkUserDbRedisPort", "").toString)
        .config("spark.redis.db", redisIndex)
        .config("spark.redis.max.pipeline.size", config.getString("redis.max.pipeline.size"))
        .config("spark.cassandra.read.timeoutMS", config.getString("cassandra.read.timeoutMS"))
        .config("spark.cassandra.query.retry.count", config.getString("cassandra.query.retry.count"))
        .config("spark.cassandra.input.consistency.level", config.getString("cassandra.input.consistency.level"))
        .config("spark.redis.scan.count", config.getString("redis.scan.count"))
        .getOrCreate()

    def filterUserData(userDF: DataFrame): DataFrame = {
      if (specificUserId != null) {
        println("Filtering for " + specificUserId)
        userDF.filter(col("id") === specificUserId)
      } else if (null != fromSpecificDate) {
        println(s"Filtering for :$fromSpecificDate ")
        userDF.filter(col("updateddate").isNull || to_date(col("updateddate"), "yyyy-MM-dd HH:mm:ss:SSSZ").geq(lit(fromSpecificDate)))
      } else {
        println("No inputs found!! Returning user data without filter")
        userDF
      }
    }

    def extractFromArrayStringFun(schoolname: String): String = {
      try {
        val str = schoolname.replaceAll("\\[", "\\\\[").replaceAll("\\]", "\\\\]")
        str
      } catch {
        case ex: Exception =>
          schoolname
      }
    }

    val extractFromArrayString = udf[String, String](extractFromArrayStringFun)

    def populateUserData() {

      // Get CustodianOrgID
      val custRootOrgId = getCustodianOrgId()
      Console.println("#### custRootOrgId ####", custRootOrgId)

      var userDF = filterUserData(spark.read.format("org.apache.spark.sql.cassandra").option("table", "user").option("keyspace", sunbirdKeyspace).load()
        .select(
          col("firstname"), col("lastname"), col("email"), col("phone"),
          col("rootorgid"), col("framework"), col("userid"), col("status")
        )
        .filter(col("userid").isNotNull))
      userDF = userDF.withColumn("framework_id", element_at(col("framework.id"), 1))

      val fwCategories = getFrameworkCategories(userDF)

      fwCategories.foreach(category => {
        userDF = userDF.withColumn(s"framework_${category}" , col(s"framework.${category}"))
      })

      userDF = userDF.drop("framework")
        .withColumn("usersignintype", when(col("rootorgid") === lit(custRootOrgId), "Self-Signed-In").otherwise("Validated"))
        .persist(StorageLevel.MEMORY_ONLY)

      Console.println("User records count:", userDF.count());
      val saveMode = if (specificUserId == null && refreshUserData.equalsIgnoreCase("true")) SaveMode.Overwrite else SaveMode.Append
      val res1 = time(populateToRedis(userDF, saveMode = saveMode)) // Insert all userData Into redis
      Console.println("Time taken to insert user records", res1._1)

      val userOrgDF = spark.read.format("org.apache.spark.sql.cassandra").option("table", "user_organisation").option("keyspace", sunbirdKeyspace).load().filter(lower(col("isdeleted")) === "false")
        .select(col("userid"), col("organisationid"))

      val organisationDF = spark.read.format("org.apache.spark.sql.cassandra").option("table", "organisation").option("keyspace", sunbirdKeyspace).load()
        .select(col("id"), col("orgname"))

      /**
       * Get a union of RootOrg and SubOrg information for a User
       */
      val userRootOrgDF = userDF
        .join(userOrgDF, userOrgDF.col("userid") === userDF.col("userid") && userOrgDF.col("organisationid") === userDF.col("rootorgid"))
        .select(userDF.col("userid"), col("rootorgid"), col("organisationid"))

      val userSubOrgDF = userDF
        .join(userOrgDF, userOrgDF.col("userid") === userDF.col("userid") && userOrgDF.col("organisationid") =!= userDF.col("rootorgid"))
        .select(userDF.col("userid"), col("rootorgid"), col("organisationid"))

      val rootOnlyOrgDF = userRootOrgDF.join(userSubOrgDF, Seq("userid"), "leftanti").select(userRootOrgDF.col("*"))
      val userOrgDenormDF = rootOnlyOrgDF.union(userSubOrgDF)

      /**
       * Resolve organization name for a RootOrg
       */
      val resolvedOrgNameDF = userOrgDenormDF
        .join(organisationDF, organisationDF.col("id") === userOrgDenormDF.col("rootorgid"), "left_outer")
        .groupBy("userid")
        .agg(concat_ws(",", collect_set("orgname")).as("orgname_resolved"))
      val filteredOrgDf = resolvedOrgNameDF.select(col("userid"), col("orgname_resolved").as("orgname"))

      val res2 = time(populateToRedis(filteredOrgDf))
      userDF.unpersist();
      Console.println("Time taken to insert user org records", res2._1)
    }

    def getCustodianOrgId(): String = {
      val systemSettingDF = spark.read.format("org.apache.spark.sql.cassandra").option("table", "system_settings").option("keyspace", sunbirdKeyspace).load()
      val df = systemSettingDF.where(col("id") === "custodianOrgId" && col("field") === "custodianOrgId").select(col("value"))
      df.select("value").first().getString(0)
    }

    def getFrameworkCategories(userDf: DataFrame): Set[String] = {
      var frameworkCategories = Set[String]()
      userDf.select("framework_id").na.drop().distinct().rdd.collect().map(row => {
        val fwID = row.getString(0)
        val apiURL = String.format("%s%s", fwReadApiUrl, fwID)
        val fwReadRes = RestUtil.get[Map[String, AnyRef]](apiURL)

        val categories = if(isValidFrameworkResponse(fwReadRes)){
          var categoriesList = List[String]()
          val categoryRes = fwReadRes.get("result").get.asInstanceOf[Map[String, AnyRef]].get("framework").get.asInstanceOf[Map[String, AnyRef]].get("categories").get.asInstanceOf[List[Map[String, AnyRef]]]
          categoryRes.foreach(category => {
            val categoryCode = category.get("code").get.asInstanceOf[String]
            categoriesList = categoriesList :+ categoryCode
          })
          categoriesList
        } else List()
        frameworkCategories = frameworkCategories ++ categories
      })
      frameworkCategories
    }

    def isValidFrameworkResponse(fwReadRes: Map[String, AnyRef]): Boolean = {
      if (fwReadRes.get("responseCode").get.asInstanceOf[String].toUpperCase.equalsIgnoreCase("OK") && fwReadRes.get("result").get.asInstanceOf[Map[String, AnyRef]].nonEmpty && fwReadRes.get("result").get.asInstanceOf[Map[String, AnyRef]].contains("framework")) true else false
    }

    def denormUserData(): Unit = {

      val userDF = filterUserData(spark.read.format("org.apache.spark.sql.cassandra").option("table", "user").option("keyspace", sunbirdKeyspace).load()
        .filter(col("userid").isNotNull))
        .select(col("userid"), col("profilelocation"), col("profileusertypes").as("profileusertypeslist")).persist(StorageLevel.MEMORY_ONLY)
      val userOrgDF = spark.read.format("org.apache.spark.sql.cassandra").option("table", "user_organisation").option("keyspace", sunbirdKeyspace).load().filter(lower(col("isdeleted")) === "false")
        .select(col("userid"), col("organisationid")).persist(StorageLevel.MEMORY_ONLY)

      val organisationDF = spark.read.format("org.apache.spark.sql.cassandra").option("table", "organisation").option("keyspace", sunbirdKeyspace).load()
        .select(col("id"), col("orgname"), col("externalid"), col("organisationtype")).persist(StorageLevel.MEMORY_ONLY)

      val locationDF = spark.read.format("org.apache.spark.sql.cassandra").option("table", "location").option("keyspace", sunbirdKeyspace).load().persist(StorageLevel.MEMORY_ONLY)

      val userDetailsDF = generateUserData(userDF, locationDF, userOrgDF, organisationDF)
      val res2 = time(populateToRedis(userDetailsDF.distinct()))
      Console.println("Time taken to insert report details", res2._1)
      userDF.unpersist()
    }

    def generateUserData(userDF: DataFrame, locationDF: DataFrame, userOrgDF: DataFrame, organisationDF: DataFrame): DataFrame = {
      implicit val sqlContext = new SQLContext(spark.sparkContext)
      val userLocationDF = getLocationDetails(userDF, locationDF)

      val profileUserTypeDF = getProfileUserType(userDF)

      val userLocationTypeDF = userLocationDF.join(profileUserTypeDF, Seq("userid"), "left")
        .drop("profilelocation", "profileusertypeslist")

      val UserPivotDF = userLocationTypeDF
        .join(userOrgDF, userOrgDF.col("userid") === userLocationTypeDF.col("userid"), "left")
        .join(organisationDF, userOrgDF.col("organisationid") === organisationDF.col("id")
          && organisationDF.col("organisationtype").equalTo(2), "left")
        .withColumn("schoolname", extractFromArrayString(organisationDF.col("orgname")))
        .withColumn("schooludisecode", organisationDF.col("externalid"))
        .select(
          userLocationTypeDF.col("userid"),
          col("schoolname"),
          col("schooludisecode"))

      val custodianUserDF = userLocationTypeDF.as("userLocDF")
        .join(UserPivotDF, Seq("userid"), "left")
        .select("userLocDF.*", "schoolname", "schooludisecode")

      custodianUserDF
    }

    /** *
     *
     * @param userDF , locationDF
     * @return userLocationDF (containing fields: userid, state,district,block,cluster)
     *
     *         INPUT:
     *         profilelocation (String)
     *         ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
     *         [{"id":"b38057d1-d41c-4b96-a548-939e335517aa","type":"district"},{"id":"1c0f7219-a0ec-4db0-83f7-e095fd2324ed","type":"block"},
     *         {"id":"30a4dcf2-8990-4fbb-acd2-577692127aba","type":"state"}]
     *
     *         LOGIC: Get the locationIds from the profilelocation string and map it with the locationDF where type=state/district/block/cluster
     *
     *         OUTPUT:
     *         +------------------------------------+------+---------+---------------+
     *         |userid                              |state |district |block          |
     *         +------------------------------------+------+---------+----------------
     *         |56c2d9a3-fae9-4341-9862-4eeeead2e9a1|Andhra|Chittooor|Chittooorblock1|
     *         +------------------------------------+------+---------+----------------
     *
     */
    def getLocationDetails(userDF: DataFrame, locationDF: DataFrame)(implicit sqlContext: SQLContext): DataFrame = {
      import sqlContext.implicits._

      val userLocationIdDF = userDF.select(col("profilelocation"), col("userid")).rdd.map(f => {
        var locList: List[String] = List()
        val userid = f.getString(1)
        if (null != f.getString(0) && f.getString(0).nonEmpty) {
          val loc = JSONUtils.deserialize[List[Map[String, String]]](f.getString(0))
          val stateIdList = loc.filter(f => f.getOrElse("type", "").equalsIgnoreCase("state")).map(f => f.getOrElse("id", ""))
          val stateId = if (!stateIdList.isEmpty) stateIdList.head else ""
          val districtIdList = loc.filter(f => f.getOrElse("type", "").equalsIgnoreCase("district")).map(f => f.getOrElse("id", ""))
          val districtId = if (!districtIdList.isEmpty) districtIdList.head else ""
          val blockIdList = loc.filter(f => f.getOrElse("type", "").equalsIgnoreCase("block")).map(f => f.getOrElse("id", ""))
          val blockId = if (!blockIdList.isEmpty) blockIdList.head else ""
          val clusterIdList = loc.filter(f => f.getOrElse("type", "").equalsIgnoreCase("cluster")).map(f => f.getOrElse("id", ""))
          val clusterId = if (!clusterIdList.isEmpty) clusterIdList.head else ""
          locList = List(stateId, districtId, blockId, clusterId)
        }
        LocationId(userid, locList)
      }).toDF

      val userExplodedLocationDF = userLocationIdDF
        .withColumn("exploded_location", explode_outer(col("locationids")))
        .select(col("userid"), col("exploded_location"), col("locationids"))

      val userStateDF = userExplodedLocationDF
        .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "state")
        .select(userExplodedLocationDF.col("userid"), col("name").as("state"))

      val userDistrictDF = userExplodedLocationDF
        .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "district")
        .select(userExplodedLocationDF.col("userid"), col("name").as("district"))

      val userBlockDF = userExplodedLocationDF
        .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "block")
        .select(userExplodedLocationDF.col("userid"), col("name").as("block"))

      val userClusterDF = userExplodedLocationDF
        .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "cluster")
        .select(userExplodedLocationDF.col("userid"), col("name").as("cluster"))

      userDF
        .join(userStateDF, Seq("userid"), "left")
        .join(userDistrictDF, Seq("userid"), "left")
        .join(userBlockDF, Seq("userid"), "left")
        .join(userClusterDF, Seq("userid"), "left")
        .select(
          userDF.col("*"),
          col("state"),
          col("district"),
          col("block"),
          col("cluster"))
    }

    /** *
     *
     * @param userDF
     * @return userProfileTypeDF (containing fields: userid, usertype, usersubtype)
     *
     *         INPUT:
     *         profileusertypelist (String)
     *         ------------------------------------------------------------------------------------------------------------------------------
     *         [{"subType":deo,"type":"teacher"}]
     *
     *         LOGIC: usertype: profileusertype.type
     *         usersubtype: profileusertype.subType
     *         profileusertypes: Stringified profileusertypelist
     *
     *         OUTPUT:
     *         +------------------------------------+---------+-------------+----------------------------------+
     *         |userid                              |usertype |usersubtype | profileusertypes                  |
     *         +------------------------------------+---------+------------+-----------------------------------+
     *         |56c2d9a3-fae9-4341-9862-4eeeead2e9a1|teacher  |deo         |[{"subType":deo,"type":"teacher"}] |
     *         +------------------------------------+---------+------------+-----------------------------------+
     *
     */

    def getProfileUserType(userDF: DataFrame)(implicit sqlContext: SQLContext): DataFrame = {
      import sqlContext.implicits._

      userDF.select(col("profileusertypeslist"), col("userid")).rdd.map { f =>
        if (null != f.getString(0) && f.getString(0).nonEmpty) {
          val profileUserTypes = JSONUtils.deserialize[List[Map[String, String]]](f.getString(0))

          val userTypeValue = mutable.ListBuffer[String]()
          val userSubtypeValue = mutable.ListBuffer[String]()
          profileUserTypes.foreach(userType => {
            val typeVal: String = userType.getOrElse("type", "")
            val subTypeVal: String = userType.getOrElse("subType", "")

            if (typeVal != null && typeVal.nonEmpty && !userTypeValue.contains(typeVal)) userTypeValue.append(typeVal)
            if (subTypeVal != null && subTypeVal.nonEmpty && !userSubtypeValue.contains(subTypeVal)) userSubtypeValue.append(subTypeVal)
          })

          ProfileUserType(f.getString(1), userTypeValue.mkString(","), userSubtypeValue.mkString(","), JSONUtils.serialize(profileUserTypes))

        } else ProfileUserType(f.getString(1), "", "", "")
      }.toDF()
    }

    def backupUserData(): Unit = {
      val userDf = populateAnonymousUserData()
      userDf.write
        .option("header", "true")
        .option("sep", ",")
        .mode("overwrite")
        .format("csv").save(config.getString("redis.user.backup.dir"))

      userDf.unpersist()

      val jedis = new Jedis(jobConfig.modelParams.get.getOrElse("sparkRedisConnectionHost", "").toString, jobConfig.modelParams.get.getOrElse("sparkUserDbRedisPort", "").toString.toInt)
      jedis.select(redisIndex.toInt)

      if (null != specificUserId) {
        jedis.del(s"user:$specificUserId");
      }
      jedis.close()
    }

    def restoreBackupData(): Unit = {
      val backupDf = spark.read.option("header", true).csv(s"${config.getString("redis.user.backup.dir")}/part-*.csv")
      indexToRedis(backupDf, config.getString("redis.user.database.index"), SaveMode.Append)
    }

    def populateToRedis(dataFrame: DataFrame, redisIndex: String = config.getString("redis.user.database.index"), saveMode: SaveMode = SaveMode.Append): Unit = {
      val filteredDF = dataFrame.filter(col("userid").isNotNull)
      val schema = filteredDF.schema
      val complexFields = schema.fields.filter(field => complexFieldTypes.contains(field.dataType.typeName))

      val resultDF = complexFields.foldLeft(filteredDF)((df, field) =>
        df.withColumn(field.name, to_json(col(field.name))))
      indexToRedis(resultDF, redisIndex, saveMode)
    }

    def indexToRedis(dataFrame: DataFrame, redisIndex: String, saveMode: SaveMode): Unit = {
      dataFrame.write
        .format("org.apache.spark.sql.redis")
        .option("host", jobConfig.modelParams.get.getOrElse("sparkRedisConnectionHost", "").toString)
        .option("port", jobConfig.modelParams.get.getOrElse("sparkUserDbRedisPort", "").toString)
        .option("dbNum", redisIndex)
        .option("table", "user")
        .option("key.column", "userid")
        .mode(saveMode)
        .save()
    }

    if (populateAnonymousData.equalsIgnoreCase("false") && refreshUserData.equalsIgnoreCase("true")) {
      val refresh1 = time(backupUserData())
      Console.println("Time taken to backup user data:", refresh1._1 / 1000);
    } else {
      Console.println("Redis data not getting cleared");
    }

    if (!populateAnonymousData.equalsIgnoreCase("true")) {
      val res1 = time(populateUserData())
      val res2 = time(denormUserData())
      val totalTimeTaken = (res1._1 + res2._1).toDouble / 1000
      Console.println("Time taken for individual steps:", "stage1", res1._1, "stage2", res2._1)
      Console.println("Time taken for complete script:", totalTimeTaken);
    } else {
      val res1 = time(populateAnonymousUserData())
      Console.println("Time taken for populate anonymous records:", res1._1);

      val res2 = time(populateToRedis(res1._2)) // Insert all userData Into redis
      Console.println("Time taken to insert anonymous records", res1._1)
      res1._2.unpersist()
      Console.println("Time taken for complete script:", (res1._1 + res2._1).toDouble / 1000);
    }

    if (populateAnonymousData.equalsIgnoreCase("false") && refreshUserData.equalsIgnoreCase("true")) {
      val refresh2 = time(restoreBackupData())
      Console.println("Time taken to restore backup data:", refresh2._1 / 1000);
    }

    def populateAnonymousUserData(anonymousDataIndex: String = redisIndex): DataFrame = {
      val anonymousDataDF = spark.read.format("org.apache.spark.sql.redis")
        .option("host", jobConfig.modelParams.get.getOrElse("sparkRedisConnectionHost", "").toString)
        .option("port", jobConfig.modelParams.get.getOrElse("sparkUserDbRedisPort", "").toString)
        .option("dbNum", anonymousDataIndex)
        .schema(
          StructType(
            Array(
              StructField("userid", StringType),
              StructField("usersignintype", StringType),
              StructField("userlogintype", StringType)
            )
          )
        )
        .option("table", "user")
        .option("key.column", "userid")
        .load().filter(col("usersignintype") === "Anonymous" || col("usersignintype").isNull).persist(StorageLevel.MEMORY_ONLY)
      Console.println("Anonymous data user count: " + anonymousDataDF.count())

      anonymousDataDF
    }

    JobLogger.end(s"${name()} completed execution", "SUCCESS", Option(Map()))
  }

  def time[R](block: => R): (Long, R) = {
    val t0 = System.currentTimeMillis()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis()
    ((t1 - t0), result)
  }

}