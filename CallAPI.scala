import oauth.signpost.commonshttp.CommonsHttpOAuthConsumer
import org.apache.http.client.HttpClient
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.commons.io.IOUtils
import org.apache.spark.{SparkConf, SparkContext}
/*** Created by mani on 11/13/2016. */
object CallAPI{
  def main(args: Array[String]) {

    val consKey = "eJxqZ4TOHItrLmyS2DWAQj7h0"
    val consSecret = "WtHaGRkVCoH1TvvmzyT1ZWLiNkgGStlsRuQG9Qt0bDpjZQGpe5"
    val accessToken = "790262378738819072-XySQD2vbTj8ynWHeFXAiKXxzT58uvER"
    val accessSecret = "m70V88ex33VN5lRJZOaSL1nNY4GzR2jqDVwZPcjSATmVB"

    val cons = new CommonsHttpOAuthConsumer(consKey,consSecret)
    cons.setTokenWithSecret(accessToken,accessSecret)
    System.setProperty("hadoop.home.dir","C:\\Users\\mani\\Desktop\\Winutils")
    val sparkConf=new SparkConf().setAppName("CallAPI").setMaster("local[*]")
    val sc=new SparkContext(sparkConf)
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)
    val dframe=sqlContext.read.json("C:\\Users\\mani\\Desktop\\Winutils\\data2.json")
    dframe.registerTempTable("dframetable")
    dframe.printSchema()
    val username=sqlContext.sql("select user.screen_name from dframetable where text like '%Awards%' order by user.followers_count desc")
    username.show()
    val usern = scala.io.StdIn.readLine()
    val getReq = new HttpGet("https://api.twitter.com/1.1/trends/available.json?screen_name="+usern)
    cons.sign(getReq)
    val client= new DefaultHttpClient()
    val results = client.execute(getReq)
    println(IOUtils.toString(results.getEntity().getContent()))

  }
}
