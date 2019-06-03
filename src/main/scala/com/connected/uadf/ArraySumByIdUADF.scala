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

package com.connected.uadf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class ArraySumByIdUADF() extends UserDefinedAggregateFunction {

  def inputSchema: StructType = new StructType().add("arrayCol", ArrayType(DoubleType))

  def bufferSchema = new StructType().add("sumArray", ArrayType(DoubleType))

  def dataType: DataType = ArrayType(DoubleType)

  def deterministic = true

  def initialize(buffer: MutableAggregationBuffer) = {
    buffer.update(0, Array(0.0, 0.0, 0.0))
  }

  def update(buffer: MutableAggregationBuffer, input: Row) = {
    val seqBuffer = buffer(0).asInstanceOf[IndexedSeq[Double]]
    val seqInput = input(0).asInstanceOf[IndexedSeq[Double]]
    buffer(0) = seqBuffer.zip(seqInput).map{ case (x, y) => x + y }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    val seqBuffer1 = buffer1(0).asInstanceOf[IndexedSeq[Double]]
    val seqBuffer2 = buffer2(0).asInstanceOf[IndexedSeq[Double]]
    buffer1(0) = seqBuffer1.zip(seqBuffer2).map{ case (x, y) => x + y }
  }

  def evaluate(buffer: Row) = {
    buffer(0)
  }

}