package org.sunbird.core.util

import org.ekstep.analytics.framework.conf.AppConf

object BatchStatus extends Enumeration {
    type BatchStatus = Value
    val Upcoming = Value(0)
    val Ongoing = Value(1)
    val Expired = Value(2)
}

object Constants {

    val env = AppConf.getConfig("cassandra.keyspace_prefix");
    val DEVICE_KEY_SPACE_NAME = env+"device_db";
    val CONTENT_KEY_SPACE_NAME = env+"content_db";
    val PLATFORM_KEY_SPACE_NAME = env+"platform_db";
    val CONTENT_STORE_KEY_SPACE_NAME = env+"content_store";
    val CONTENT_DATA_TABLE = "content_data";
    val REGISTERED_TAGS = "registered_tags";
    val WORKFLOW_USAGE_SUMMARY = "workflow_usage_summary";
    val WORKFLOW_USAGE_SUMMARY_FACT = "workflow_usage_summary_fact";
    val DEVICE_PROFILE_TABLE = env+"device_profile";
    val EXPERIMENT_DEFINITION_TABLE = "experiment_definition";

    val SEARCH_SERVICE_URL = AppConf.getConfig("service.search.url")
    val COMPOSITE_SEARCH_URL = s"$SEARCH_SERVICE_URL" + AppConf.getConfig("service.search.path")
    val TENANT_PREFERENCE_PRIVATE_READ_URL = AppConf.getConfig("tenant.pref.read.private.api.url")
    val ORG_PRIVATE_SEARCH_URL: String = AppConf.getConfig("org.search.private.api.url")
    val TEMP_DIR = AppConf.getConfig("spark_output_temp_dir")
    val SUNBIRD_COURSES_KEY_SPACE = AppConf.getConfig("course.metrics.cassandra.sunbirdCoursesKeyspace")
}