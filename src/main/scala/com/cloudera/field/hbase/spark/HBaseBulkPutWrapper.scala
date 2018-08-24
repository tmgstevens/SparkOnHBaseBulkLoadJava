package com.cloudera.field.hbase.spark

import java.util.Map

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.spark._
import org.apache.hadoop.hbase.util.Pair
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.Function

/*
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


class HBaseBulkPutWrapper(@transient val hbc: JavaHBaseContext) extends Serializable {
  val hbaseContext: HBaseContext = hbc.hbaseContext

  /**
    * A simple abstraction over the HBaseContext.bulkLoad method.
    * It allow addition support for a user to take a JavaRDD and
    * convert into new JavaRDD[Pair] based on MapFunction,
    * and HFiles will be generated in stagingDir for bulk load
    *
    * @param javaRdd                        The javaRDD we are bulk loading from
    * @param tableName                      The HBase table we are loading into
    * @param mapFunc                        A Function that will convert a value in JavaRDD
    *                                       to Pair(KeyFamilyQualifier, Array[Byte])
    * @param stagingDir                     The location on the FileSystem to bulk load into
    * @param familyHFileWriteOptionsMap     Options that will define how the HFile for a
    *                                       column family is written
    * @param compactionExclude              Compaction excluded for the HFiles
    * @param maxSize                        Max size for the HFiles before they roll
    */
  def bulkLoad[T](javaRdd: JavaRDD[T],
                  tableName: TableName,
                  mapFunc : Function[T, Pair[KeyFamilyQualifier, Array[Byte]]],
                  stagingDir: String,
                  familyHFileWriteOptionsMap: Map[Array[Byte], FamilyHFileWriteOptions],
                  compactionExclude: Boolean,
                  maxSize: Long):
  Unit = {
    hbaseContext.bulkLoad[Pair[KeyFamilyQualifier, Array[Byte]]](javaRdd.map(mapFunc).rdd, tableName, t => {
      val keyFamilyQualifier = t.getFirst
      val value = t.getSecond
      Seq((keyFamilyQualifier, value)).iterator
    }, stagingDir, familyHFileWriteOptionsMap, compactionExclude, maxSize)
  }

  /**
    * A simple abstraction over the HBaseContext.bulkLoadThinRows method.
    * It allow addition support for a user to take a JavaRDD and
    * convert into new JavaRDD[Pair] based on MapFunction,
    * and HFiles will be generated in stagingDir for bulk load
    *
    * @param javaRdd                        The javaRDD we are bulk loading from
    * @param tableName                      The HBase table we are loading into
    * @param mapFunc                        A Function that will convert a value in JavaRDD
    *                                       to Pair(ByteArrayWrapper, FamiliesQualifiersValues)
    * @param stagingDir                     The location on the FileSystem to bulk load into
    * @param familyHFileWriteOptionsMap     Options that will define how the HFile for a
    *                                       column family is written
    * @param compactionExclude              Compaction excluded for the HFiles
    * @param maxSize                        Max size for the HFiles before they roll
    */
  def bulkLoadThinRows[T](javaRdd: JavaRDD[T],
                          tableName: TableName,
                          mapFunc : Function[T, Pair[ByteArrayWrapper, FamiliesQualifiersValuesJ]],
                          stagingDir: String,
                          familyHFileWriteOptionsMap: Map[Array[Byte], FamilyHFileWriteOptions],
                          compactionExclude: Boolean,
                          maxSize: Long):
  Unit = {
    hbaseContext.bulkLoadThinRows[Pair[ByteArrayWrapper, FamiliesQualifiersValuesJ]](javaRdd.map(mapFunc).rdd,
      tableName, t => {
        (t.getFirst, t.getSecond)
      }, stagingDir, familyHFileWriteOptionsMap, compactionExclude, maxSize)
  }

}
