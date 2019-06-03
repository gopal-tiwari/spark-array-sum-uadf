/*
 *
 *  * Copyright  2019 Gopal Tiwari
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */


import com.connected.uadf.ArraySumByIdUADF
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, DoubleType}
import org.apache.log4j.{Level, Logger}

object UadfUsesExample
{

  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("spark").setLevel(Level.OFF)
    Logger.getLogger("hive").setLevel(Level.OFF)
    Logger.getLogger("hadoop").setLevel(Level.OFF)
    Logger.getLogger("hdfs").setLevel(Level.OFF)
    Logger.getRootLogger().setLevel(Level.OFF)

    val sc = SparkSession.builder
      .appName("uadf example").master("local[2]")
      .getOrCreate()

    import sc.implicits._

    val df = Seq(
      (1, Array(22.2, 234.5, 69.5)),
      (1, Array(13.2, 58.5, 23.34)),
      (2, Array(58.85, 23.73, 34.5)),
      (2, Array(135.145, 67.5456, 6.235))
    ).toDF("id", "arrayCol")


    val fun = new ArraySumByIdUADF()

    df.select($"id", $"arrayCol" cast ArrayType(DoubleType))
      .groupBy("id").agg(fun($"arrayCol"))
      .show(false)

    println("")
    df.printSchema()
  }

}
