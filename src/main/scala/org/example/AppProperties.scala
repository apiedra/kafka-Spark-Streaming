package org.example

import java.util.ResourceBundle

class AppProperties() {
  var resource: ResourceBundle = _

  def initProperties(env:String): Unit = {
    resource = ResourceBundle.getBundle("application")

  }


  object KafkaProperties {
    val topicRoot: String = resource.getString("topic.root")
    val topicName: String = resource.getString("topic.name")
    val bootstrapServers: String = resource.getString("topic.bootstrap_servers")
    val groupId: String = resource.getString("topic.groupId")
    val keyDeserializer: String = resource.getString("topic.keyDeserializer")
    val valueDeserializer: String = resource.getString("topic.valueDeserializer")
    val autoCommitInterval: String = resource.getString("topic.autoCommitIntervalMs")
    val sessionTimeout: String = resource.getString("topic.sessionTimeoutMs")
    val autoOffset: String = resource.getString("topic.autoOffseReset")
    val enableAutoCommit: Boolean = resource.getString("topic.enableAutoCommit").toBoolean
  }
}


object AppProperties {
  private var _instance: AppProperties = null

  def instance() = {
    if (_instance == null)
      _instance = new AppProperties()
    _instance
  }
}
