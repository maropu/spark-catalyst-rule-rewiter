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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.optimizer.CollapseProject
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext


class VarianceThresholdSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    // Sets user-defined optimization rules for feature selection
    sqlContext.experimental.extraOptimizations = Seq(VarianceThreshold, CollapseProject)
  }

  test("filter out features with low variances") {
    withSQLConf(
        SQLConf.CBO_ENABLED.key -> "true",
        SQLConf.HISTOGRAM_ENABLED.key -> "true",
        CatalystConf.FEATURE_SELECTION_ENABLED.key -> "true",
        CatalystConf.VARIANCE_THRESHOLD_ENABLED.key -> "true",
        CatalystConf.VARIANCE_THRESHOLD_VALUE.key -> "0.1") {

      withTable("t") {
        withTempDir { dir =>
          val data = Seq(
            (1, "one", 1.0, 1.0),
            (1, "two", 1.1, 2.3),
            (1, "three", 0.9, 3.5)
          )
          data.toDF("c0", "c1", "c2", "c3").write.mode("overwrite").parquet(dir.getAbsolutePath)
          spark.read.parquet(dir.getAbsolutePath).write.saveAsTable("t")
          spark.sql("ANALYZE TABLE t COMPUTE STATISTICS FOR COLUMNS c0, c1, c2, c3")

          // We assume that Catalyst filters out `c0` and `c2` because of low variances
          val df = sql("SELECT c0, * FROM t")
          df.printSchema()
          val optimizedPlan = df.queryExecution.optimizedPlan
          assert(optimizedPlan.output.map(_.name) === Seq("c1", "c3"))
          df.show
          df.explain(true)
          checkAnswer(df, Row("one", 1.0) :: Row("two", 2.3) :: Row("three", 3.5) :: Nil)
        }
      }
    }
  }
}
