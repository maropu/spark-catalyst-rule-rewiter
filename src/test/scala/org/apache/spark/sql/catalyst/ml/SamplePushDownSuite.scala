/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.catalyst.ml

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext


class SamplePushDownSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    // Sets user-defined optimization rules for feature selection
    sqlContext.experimental.extraOptimizations = Seq(SamplePushDown)
  }

  test("sample pushdown") {
    withSQLConf(
        SQLConf.CBO_ENABLED.key -> "true",
        SQLConf.HISTOGRAM_ENABLED.key -> "true",
        CatalystConf.FEATURE_SELECTION_ENABLED.key -> "true",
        CatalystConf.SAMPLE_PUSHDOWN_ENABLED.key -> "true") {

      withTable("r1", "r2") {
        withTempDir { dir =>
          Seq((1, 1, 3.8), (2, 1, 1.1), (3, 2, 0.9), (4, 1, 0.9))
            .toDF("pk1", "fk1", "a0").write.mode("overwrite").parquet(dir.getAbsolutePath)
          spark.read.parquet(dir.getAbsolutePath).write.saveAsTable("r1")
          spark.sql("ANALYZE TABLE r1 COMPUTE STATISTICS FOR COLUMNS fk1")

          Seq((1, "abc"), (2, "def"))
            .toDF("pk2", "b0").write.mode("overwrite").parquet(dir.getAbsolutePath)
          spark.read.parquet(dir.getAbsolutePath).write.saveAsTable("r2")
          spark.sql("ANALYZE TABLE r2 COMPUTE STATISTICS FOR COLUMNS pk2")

          // Two join case
          val df = spark.sql(
            s"""SELECT * FROM (
               |  SELECT * FROM r1, r2 WHERE fk1 = pk2 AND pk1 > 3
               |) TABLESAMPLE (25 PERCENT)
             """.stripMargin)
          df.explain()

          // Three join case
        }
      }
    }
  }
}
