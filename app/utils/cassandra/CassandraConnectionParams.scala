package utils.cassandra

import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions._

object CassandraConnectionParams {

  private val cassandraConfig = ConfigFactory.load.getConfig("cassandra")

  val port = cassandraConfig.getInt("port")
  val hosts = cassandraConfig.getStringList("hosts").toList
  val keyspace = cassandraConfig.getString("keyspace")
  val replicationFactor = cassandraConfig.getString("replication-factor").toInt
  val readConsistency = cassandraConfig.getString("read-consistency")
  val writeConsistency = cassandraConfig.getString("write-consistency")

}
