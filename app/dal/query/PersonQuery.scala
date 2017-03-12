package dal.query

trait PersonQuery {

  /**
   * Query to store person's details in database
   *
   * @param id person's id
   * @param name person's name
   * @param age person's age
   */
  def createPersonQuery(id: Long, name: String, age: Int, tableName: String) = {
    s"INSERT INTO $tableName (id, name, age) VALUES ($id, '$name', $age)"
  }

  /**
   * Query to fetch list of all persons
   */
  def getPersons(tableName: String) = {
    s"SELECT * FROM $tableName"
  }

}
