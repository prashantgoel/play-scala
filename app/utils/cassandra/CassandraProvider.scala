package utils.cassandra

import com.datastax.driver.core._
import play.api.Logger

/**
 * Creates single copy of session to be used across the application.
 */
object CassandraProvider {

  // Session for Eventuate keyspace
  val session = {
    val defaultConsistencyLevel = ConsistencyLevel.valueOf(CassandraConnectionParams.writeConsistency)
    createSessionAndInitKeyspace(CassandraConnectionParams.keyspace, defaultConsistencyLevel).get
  }

  // Shutdown hook clears up connections in case of app shutdown.
  sys addShutdownHook {
    Logger.info("Shutdown hook caught.Closing Cassandra session and cluster")
//    if (session.isDefined) {
      val cluster = session.getCluster
      session.close()
      cluster.close()
//    }
    Logger.info("Shutdown hook executed successfully")
  }

  /**
   * This function connects to the cluster and return the session connecting to the specific keyspace.
   * If keyspace is missing it will create.
   *
   * @param keySpace
   * @param defaultConsistencyLevel
   * @return
   */
  private def createSessionAndInitKeyspace(keySpace: String, defaultConsistencyLevel: ConsistencyLevel = ConsistencyLevel.QUORUM): Option[Session] = {
    val cluster = new Cluster.Builder().
      addContactPoints(CassandraConnectionParams.hosts.toArray: _*).
      withPort(CassandraConnectionParams.port).
      withQueryOptions(new QueryOptions().setConsistencyLevel(defaultConsistencyLevel)).build
    val session = connectToKeyspace(keySpace, cluster)
    Some(session)
  }

  private def connectToKeyspace(keySpace: String, cluster: Cluster): Session = {
    val session = cluster.connect
    val keyspaceQueryResult: ResultSet = session.execute(s"SELECT * FROM system_schema.keyspaces where keyspace_name='${keySpace}'")
    if (keyspaceQueryResult.isExhausted) {
      session.execute(s"CREATE KEYSPACE ${keySpace} WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : ${CassandraConnectionParams.replicationFactor} }")
    }
    session.execute(s"USE ${keySpace}")
    createPersonTables(keySpace, session)
    session
  }

  private def createPersonTables(keySpace: String, session: Session): ResultSet = {
    session.execute(s"CREATE TABLE IF NOT EXISTS ${keySpace}.people ( id bigint, name Text, age int, PRIMARY KEY (id) )")
  }

}