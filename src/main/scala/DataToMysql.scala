import java.sql.{Connection, Driver, DriverManager}

object test{
  def main(args: Array[String]): Unit = {
    var data = new DataToMysql()
    var conn = data.getConnection
    var ps = conn.prepareStatement("insert into users(username,pwd) values(?,?)")
    ps.setString(1,"li")
    ps.setString(2,"qi")
    ps.execute()
  }
}

class DataToMysql {
  val IP = "localhost"
  val Port = "3306"
  val DBType = "mysql"
  val DBName = "scrapy"
  val username = "root"
  val password = "000000"
  val url: String = "jdbc:" + DBType + "://" + IP + ":" + Port + "/" + DBName+"?serverTimezone=GMT"
  classOf[com.mysql.jdbc.Driver]

  def write(map:Map[String,String]):Unit={

  }

  def getConnection: Connection = {
    DriverManager.getConnection(url, username, password)
  }

  def close(conn: Connection): Unit = {
    try {
      if (!conn.isClosed || conn != null) {
        conn.close()
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
  }

}
