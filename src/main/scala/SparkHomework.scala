import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

case class Student(id: Int, name: String, age: Int, sex: String, subject: String, score: Int)

object SparkHomework {
  def main(args: Array[String]): Unit = {
    //sc: SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("Score").setMaster("local[2]")

    val sc: SparkContext = new SparkContext(sparkConf)

    // 1. 读取文件的数据test.txt
    val dataRDD: RDD[String] = sc.textFile(this.getClass.getClassLoader.getResource("test.txt").getPath);

    val allSts = dataRDD.map(x => {
      val line = x.split(" ")
      Student(line(0).toInt, line(1), line(2).toInt, line(3), line(4), line(5).toInt)
    })

    // 2. 一共有多少个小于20岁的人参加考试？
    val qu2 = allSts.map(x => x.age).filter(age => age < 20).count();
    println("小于20岁的人数："+ qu2);

    //3. 一共有多少个等于20岁的人参加考试？
    val qu3 = allSts.map(x => x.age).filter(age => age == 20).count();
    println("等于20岁的人数："+ qu3);

    // 4. 一共有多少个大于20岁的人参加考试？
    val qu4 = allSts.map(x => x.age).filter(age => age > 20).count();
    println("大于20岁的人数："+ qu4);

    // 5. 一共有多个男生参加考试？
    val qu5 = allSts.map(x => x.sex).filter(sex => sex == "男").count();
    println("男生人数：" + qu5);

    // 6. 一共有多少个女生参加考试？
    val qu6 = allSts.map(x => x.sex).filter(sex => sex == "女").count();
    println("女生人数：" + qu5);

    // 7. 12班有多少人参加考试？
    val qu7 = allSts.map(x => x.id).filter(id => id == 12).count();
    println("12班有多少人：" + qu7)

    // 8. 13班有多少人参加考试？
    val qu8 = allSts.map(x => x.id).filter(id => id == 13).count();
    println("13班有多少人：" + qu8)

    // 9. 语文科目的平均成绩是多少？
    val allChinese = allSts.map(x => (x.subject, x.score)).filter(kv => kv._1 == "chinese").map(m =>m._2)
    println("语文科目的平均成绩: " + allChinese.reduce(_+_)/allChinese.count())

    // 10. 数学科目的平均成绩是多少？
    val allMath = allSts.map(x => (x.subject, x.score)).filter(kv => kv._1 == "math").map(m => m._2)
    println("数学科目的平均成绩: " + allMath.reduce(_ + _) / allMath.count())

    // 11. 英语科目的平均成绩是多少？
    val allEng= allSts.map(x => (x.subject, x.score)).filter(kv => kv._1 == "english").map(m => m._2)
    println("英语科目的平均成绩: " + allEng.reduce(_ + _) / allEng.count())

    // 12. 每个人平均成绩是多少？
    val personScore = allSts.groupBy(x => x.name).map(kv => {
      val allScore = kv._2.map(s => s.score);
      (kv._1, allScore.sum/allScore.size)
    })
    println("每个人平均成绩: " + personScore.collect().mkString(","))

    // 13. 12班平均成绩是多少？
    val sub12Score = allSts.groupBy(x => x.id).filter(kv =>kv._1 == 12).map(kv => {
      val allScore = kv._2.map(s => s.score);
      (kv._1, allScore.sum / allScore.size)
    })
    println("12班平均成绩: " + sub12Score.collect().mkString(","))

    // 14. 12班男生平均总成绩是多少？
    val sub12BoyScore = allSts.groupBy(x => (x.id, x.sex)).filter(kv => kv._1._1 == 12 && kv._1._2 == "男").map(kv => {
      val allScore = kv._2.map(s => s.score);
      (kv._1, allScore.sum / allScore.size)
    })
    println("12班男生平均成绩: " + sub12BoyScore.collect().mkString(","))

    // 15. 12班女生平均总成绩是多少？
    val sub12GirlScore = allSts.groupBy(x => (x.id, x.sex)).filter(kv => kv._1._1 == 12 && kv._1._2 == "女").map(kv => {
      val allScore = kv._2.map(s => s.score);
      (kv._1, allScore.sum / allScore.size)
    })
    println("12班女生平均成绩: " + sub12GirlScore.collect().mkString(","))

    // 16. 13班平均成绩是多少？
    val sub13Score = allSts.groupBy(x => x.id).filter(kv => kv._1 == 13).map(kv => {
      val allScore = kv._2.map(s => s.score);
      (kv._1, allScore.sum / allScore.size)
    })
    println("13班平均成绩: " + sub12Score.collect().mkString(","))

    // 17.13 班男生平均总成绩是多少？
    val sub13BoyScore = allSts.groupBy(x => (x.id, x.sex)).filter(kv => kv._1._1 == 13 && kv._1._2 == "男").map(kv => {
      val allScore = kv._2.map(s => s.score);
      (kv._1, allScore.sum / allScore.size)
    })
    println("13班男生平均成绩: " + sub13BoyScore.collect().mkString(","))

    // 18 13 班女生平均总成绩是多少？
    val sub13GirlScore = allSts.groupBy(x => (x.id, x.sex)).filter(kv => kv._1._1 == 13 && kv._1._2 == "女").map(kv => {
      val allScore = kv._2.map(s => s.score);
      (kv._1, allScore.sum / allScore.size)
    })
    println("13班女生平均成绩: " + sub13GirlScore.collect().mkString(","))

    // 19. 全校语文成绩最高分是多少？
    println("全校语文成绩最高分: " + allSts.map(x => (x.subject, x.score)).filter(kv => kv._1 == "chinese").map(kv => kv._2).max)

    // 20. 12班语文成绩最低分是多少？
    println("12班语文成绩最低分: " + allSts.map(x => (x.id, x.subject, x.score)).filter(kv => kv._1 == 12 && kv._2 == "chinese").map(kv => kv._3).min)

    // 21. 13班数学最高成绩是多少？
    println("13班数学最高成绩: " + allSts.map(x => (x.id, x.subject, x.score)).filter(kv => kv._1 == 13 && kv._2 == "math").map(kv => kv._3).max)

    // 22. 总成绩大于150分的12班的女生有几个？
    val qu22 = allSts.map(x => (x.id, x.name, x.sex, x.score)).filter(kv => kv._1 == 12 && kv._3 == "女").groupBy(kv => kv._2)
      .filter(kv => {
        kv._2.map(s => s._4).sum > 150
      }).count()

    println("总成绩大于150分的12班的女生: "+ qu22)

    // 23. 总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？
    val qu23 = allSts.map(x => (x.id, x.name, x.age, x.subject, x.score)).filter(kv => kv._3 >= 19).groupBy(kv => kv._2)
      .filter(kv => {
        val total = kv._2.map(s => s._5).sum
        val mathTotal = kv._2.filter(s => s._4 == "math").map(s => s._5).sum
//        println("总分: "+total+ " 数学："+mathTotal)
        total > 150 && mathTotal >= 70
      }).map(kv => (kv._1, kv._2.map(s => s._5).sum /kv._2.map(s => s._5).size))

    println("总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩: "+ qu23.collect().mkString(","))

    sc.stop()
  }
}
