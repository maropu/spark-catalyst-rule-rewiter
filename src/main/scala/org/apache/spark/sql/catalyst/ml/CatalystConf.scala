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

import scala.language.implicitConversions

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry, ConfigReader}
import org.apache.spark.sql.internal.SQLConf

object CatalystConf {

  /**
   * Implicitly injects the [[CatalystConf]] into [[SQLConf]].
   */
  implicit def SQLConfToCatalystConf(conf: SQLConf): CatalystConf = new CatalystConf(conf)

  private val sqlConfEntries = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, ConfigEntry[_]]())

  private def register(entry: ConfigEntry[_]): Unit = sqlConfEntries.synchronized {
    require(!sqlConfEntries.containsKey(entry.key),
      s"Duplicate SQLConfigEntry. ${entry.key} has been registered")
    sqlConfEntries.put(entry.key, entry)
  }

  // For testing only
  // TODO: Need to add tests for the configurations
  private[sql] def unregister(entry: ConfigEntry[_]): Unit = sqlConfEntries.synchronized {
    sqlConfEntries.remove(entry.key)
  }

  def buildConf(key: String): ConfigBuilder = ConfigBuilder(key).onCreate(register)

  val FEATURE_SELECTION_ENABLED = buildConf("spark.sql.catalyst.featureSelection.enabled")
    .doc("Whether query rewriting rules for feature selection are enabled.")
    .booleanConf
    .createWithDefault(false)

  val VARIANCE_THRESHOLD_ENABLED =
    buildConf("spark.sql.catalyst.featureSelection.varianceThreshold.enabled")
      .doc("When true, this rule removes features with low variance; it removes all features " +
        "whose variance doesn't meet some threshold.")
      .booleanConf
      .createWithDefault(true)

  val VARIANCE_THRESHOLD_VALUE =
    buildConf("spark.sql.catalyst.featureSelection.varianceThreshold.value")
      .doc("Threshold to remove features with low variance.")
      .doubleConf
      .createWithDefault(0.05)

  val SAMPLE_PUSHDOWN_ENABLED =
    buildConf("spark.sql.catalyst.featureSelection.samplePushDown.enabled")
      .doc("When true, this rule pushes down Sample beneath the inputs of inner/outer joins " +
        "under some conditions.")
      .booleanConf
      .createWithDefault(true)
}

class CatalystConf(conf: SQLConf) {
  import CatalystConf._

  private val reader = new ConfigReader(conf.settings)

  def featureSelectionEnabled: Boolean = getConf(FEATURE_SELECTION_ENABLED)

  def varianceThresholdEnabled: Boolean = getConf(VARIANCE_THRESHOLD_ENABLED)

  def varianceThresholdValue: Double = getConf(VARIANCE_THRESHOLD_VALUE)

  def samplePushDownEnabled: Boolean = getConf(SAMPLE_PUSHDOWN_ENABLED)

  /**
   * Return the value of configuration property for the given key. If the key is not set yet,
   * return `defaultValue` in [[ConfigEntry]].
   */
  private def getConf[T](entry: ConfigEntry[T]): T = {
    require(sqlConfEntries.get(entry.key) == entry || SQLConf.staticConfKeys.contains(entry.key),
      s"$entry is not registered")
    entry.readFrom(reader)
  }
}
