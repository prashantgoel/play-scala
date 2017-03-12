package dal

import java.util.Random
import javax.inject.{Inject, Singleton}

import com.datastax.driver.core.Row
import dal.query.PersonQuery
import models.Person
import utils.cassandra.CassandraProvider._

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
 * A repository for people.
 */
@Singleton
class PersonRepository @Inject()()(implicit ec: ExecutionContext) extends PersonQuery {

  val random = new Random();

  /**
   * This method will persist person details.
   *
   * @param name person's name
   * @param age person's age
   * @return Person's object with person's id
   */
  def create(name: String, age: Int): Future[Person] = {
    val id = random.nextInt(99999).toLong
    session.execute(createPersonQuery(id, name, age, "employee.people"))
    Future(Person(id.toLong, name, age))
  }

  /**
   * This method will fetch list of all persons from database.
   *
   * @return list of persons
   */
  def list(): Future[Seq[Person]] = {
    val rows: List[Row] = session.execute(getPersons("employee.people")).all().asScala.toList
    Future(rows.map { row =>
      Person(row.getLong("id"), row.getString("name"), row.getInt("age"))
    })
  }

}
