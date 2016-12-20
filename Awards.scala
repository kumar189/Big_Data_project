import org.apache.spark.sql
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

/**
  * Created by mani on 11/4/2016.
  */

object Awards{
  def main(args: Array[String]): Unit ={

    System.setProperty("hadoop.home.dir","C:\\Users\\mani\\Desktop\\Winutils")
    val sparkConf=new SparkConf().setAppName("Awards").setMaster("local[*]")
    val sc=new SparkContext(sparkConf)
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)

    val dframe=sqlContext.read.json("C:\\Users\\mani\\Desktop\\Winutils\\data2.json")

    dframe.registerTempTable("dframetable")
//    dframe.printSchema()

    //val dframe2=sqlContext.read.json("C:\\Users\\mani\\Desktop\\Winutils\\data2.json")
//    val rddData=sc.textFile("C:\\Users\\mani\\Desktop\\Winutils\\tweets1.txt")
//
//    val jan=rddData.filter(l=>l.contains("#Jan"))
//    val feb=rddData.filter(l=>l.contains("#Feb"))
//    val mar=rddData.filter(l=>l.contains("#Mar"))
//    val apr=rddData.filter(l=>l.contains("#Apr"))
//    val rdd1=jan.union(feb).union(mar).union(apr).count()
//
//    val may=rddData.filter(l=>l.contains("#May"))
//    val jun=rddData.filter(l=>l.contains("#Jun"))
//    val jul=rddData.filter(l=>l.contains("#Jul"))
//    val aug=rddData.filter(l=>l.contains("#Aug"))
//    val rdd2=may.union(jun).union(jul).union(aug).count()
//
//    val sep=rddData.filter(l=>l.contains("#Sep"))
//    val oct=rddData.filter(l=>l.contains("#Oct"))
//    val nov=rddData.filter(l=>l.contains("#Nov"))
//    val dec=rddData.filter(l=>l.contains("#Dec"))
//    val rdd3=sep.union(oct).union(nov).union(dec).count()
//
//    println("Number of tweets in JAN, FEB, MAR, APR:%s".format(rdd1))
//    println("Number of tweets in MAY, JUN, JUL, AUG:%s".format(rdd2))
//    println("Number of tweets in SEP, OCT, NOV DEC:%s".format(rdd3))
//

    val hTags= sqlContext.read.json("C:\\Users\\mani\\Desktop\\Winutils\\hashtags.txt")
    val hTagsdframe=hTags.toDF().withColumnRenamed("_corrupt_record","name")
    val hashTable= hTagsdframe.registerTempTable("hashTable")


    //val abc = sqlContext.sql("select name from hashTable where name = '#HipHopAwards' ")
    //abc.show()
    // hTagsdframe.printSchema()
   // val hashquery=sqlContext.sql("select t.text as text,d.name as hashtags from dframetable t JOIN hashTable d on t.text like '%d.name%' ")
   val hashquery=sqlContext.sql("select t.text as text,d.name as hashtags from dframetable t JOIN hashTable d on t.text like '#HipHopAwards' ")
    hashquery.show()

    //    dframe2.registerTempTable("dframetable2")
    //    dframe2.printSchema()
    //val place2= rddData.filter(l=>l.contains("#Starwars"))
//    val place1= rddData.filter(li=>li.contains("#Arts"))
//    val x=place1.union(place2)
//    x.collect().foreach(println)
    //println(placequery1)


 //   val placequery3 = sqlContext.sql("select  user.screen_name as mama_tweeters from dframetable where text Like '%mama%'")
    //placequery3.show()
 //   val placequery4 = sqlContext.sql("select  user.screen_name as oscar_tweeters from dframetable where text Like '%Oscar%'")
    //placequery4.show()

//   val a=placequery1.(placequery2,"time_zone")
//   a.show()
    //val b=placequery3.join(placequery4)
//    b.show()
  //  val c=a.join(b)
   // c.show()


 /* SUCCUSSFUL QUERY ONE

    val japanT=sqlContext.sql("select text from dframetable where user.location='Japan' and text like '%Awards%' ")
    val japanTC=japanT.count()

    val UST=sqlContext.sql("select text from dframetable where user.location='US' and text like '%Awards%'")
    val USTC=UST.count()

    val taiwanT=sqlContext.sql("select text from dframetable where user.location='Taiwan'and text like '%Awards%' ")
    val taiwanTC=taiwanT.count()

    val UKT=sqlContext.sql("select text from dframetable where user.location='UK' and text like '%Awards%'")
    val UKTC=UKT.count()

    println("number of tweets from japan : "+japanTC)
    println("number of tweets from US : "+USTC)
    println("number of tweets from taiwan : "+taiwanTC)
    println("number of tweets from UK : "+UKTC)
    println("Country with maximum tweets about Awards : ")

    if(japanTC > USTC && japanTC > taiwanTC && japanTC > UKTC ){
      println("AWARDS HAS MORE TWEETS FROM JAPAN")
    }
    if(USTC > japanTC && USTC > taiwanTC && USTC > UKTC ){
      println("AWARDS HAS MORE TWEETS FROM US")
    }
    if(taiwanTC > USTC && taiwanTC > japanTC && taiwanTC > UKTC ){
      println("AWARDS HAS MORE TWEETS FROM JAPAN")
    }
    if(UKTC > japanTC && UKTC > taiwanTC && UKTC > USTC ){
      println("AWARDS HAS MORE TWEETS FROM UK")
    }


    SUCCESSFUL QUERY TWO

    val queryyy=sqlContext.sql("select upper(user.screen_name) as username,user.name, user.lang, user.location,user.followers_count from dframetable where text LIKE '%Oscar%' and user.time_zone='Pacific Time (US & Canada)' order by user.followers_count desc limit 5")
    queryyy.show()


    SUCCESSFUL RDD ONE


    val rddData=sc.textFile("C:\\Users\\mani\\Desktop\\Winutils\\tweets1.txt")

    val mama=rddData.filter(line=>line.contains("#MAMA")).count()
    val mtv=rddData.filter(line=>line.contains("#MTVEMA")).count()
    val oscar=rddData.filter(line=>line.contains("#Oscar")).count()
    val choice=rddData.filter(line=>line.contains("#Choice")).count()
    val emmy=rddData.filter(line=>line.contains("#EmmyAwards")).count()
    val pride=rddData.filter(line=>line.contains("#prideofbritain")).count()

    println("LIST OF AWARDS COUNT")
    println("mama awards tweets :%s".format(mama))
    println("mtvema awards tweets :%s".format(mtv))
    println("oscar awards tweets :%s".format(oscar))
    println("choice awards tweets :%s".format(choice))
    println("emmy awards tweets  :%s".format(emmy))
    println("pride awards tweets :%s".format(pride))

    // SUCCESSFUL RDD 2

     val rddData=sc.textFile("C:\\Users\\mani\\Desktop\\Winutils\\tweets1.txt")
     val mamacount = rddData.filter(line=>line.contains("#mama")).count()
    println(mamacount)
     val oscarcount=rddData.filter(l=>l.contains("#Emmy")).count()
    println(oscarcount)
     if(mamacount.compareTo(oscarcount)==1){
       println("mama is more written in Twitter than emmy")
     }
    else
     {
       println("emmy is more written in Twitter than mama")
     }

*/

//    val r1 = sqlContext.sql("select created_at, count(*) as count from dframetable group by created_at order by count")
//    r1.show()
//    val r1 = sqlContext.sql("SELECT user.name,  ,count(*) as count FROM dframetable WHERE ='HEART STROKE' group by UserName order by count desc limit 1")
//    val r2 = sqlContext.sql("SELECT user.name,'CANCER' as diseaseType,count(*) as count FROM disCat2 WHERE diseaseType='CANCER' group by UserName order by count desc limit 1 ")
    /*success RDD
    val awardsCount=rddData.filter(line=>line.contains("#Awards")).count()
    println("Count:: %s".format(awardsCount))*/
    /* SUCCESS ONE
      val placequery=sqlContext.sql("select user.name, user.location, user.followers_count from dframetable where user.location='Japan' UNION ALL select user.name,user.location,user.followers_count from dframetable2 where user.followers_count>100 ")
      placequery.show()
    */

//     SUCCESS THREE
    /*val placequery3 = sqlContext.sql("select user.time_zone, user.location from tframetable where user.time_zone is not null ")
    placequery3.show()*/


/* SUCCESS TWO
    val placequery2=sqlContext.sql("select upper(user.screen_name), user.location, user.description, user.followers_count, user.friends_count, user.created_at from dframetable where user.followers_count > 100 and user.location is not null order by user.name desc ")
    placequery2.show()
    placequery2.printSchema()
*/
    /* SUCCESS FOUR

   val grpq4=sqlContext.sql("select user.name, user.location,user.followers_count from dframetable2 where user.name is not null and (user.location='Japan' or user.location='China') limit 5")
    grpq4.show()

     */

    /*  SUCCESS FIVE
     val grpq5=sqlContext.sql("select user.name, user.location,user.followers_count from dframetable2 where user.name is not null and user.location like 'I%' limit 10 ")
    grpq5.show()

     */

  }
}
