/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.ml

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

trait SharedSparkSessionWithExtenstion extends SQLTestUtils {
  type ExtensionsBuilder = SparkSessionExtensions => Unit

  protected def sparkConf = {
    new SparkConf()
      .setAppName("test-sql-context")
      .setMaster("local[1]")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
  }

  /**
   * The [[SparkSession]] to use for all tests in this suite and the underlying
   * [[org.apache.spark.SparkContext]] will be run in local mode
   * with the default test configurations.
   */
  private var _spark: SparkSession = null

  /**
   * The [[SparkSession]] to use for all tests in this suite.
   */
  protected implicit def spark: SparkSession = {
    assert(_spark != null)
    _spark
  }

  protected def initializeSession(builder: ExtensionsBuilder): Unit = {
    if (_spark == null) {
      SparkSession.cleanupAnyExistingSession()
      _spark = SparkSession.builder
        .config(sparkConf)
        .withExtensions(builder)
        .getOrCreate()
    }
  }

  /**
   * Stop the underlying [[org.apache.spark.SparkContext]], if any.
   */
  protected override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      try {
        if (_spark != null) {
          try {
            _spark.sessionState.catalog.reset()
          } finally {
            _spark.stop()
            _spark = null
          }
        }
      } finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }
  }
}
