/*
 * This file is part of the "silex" library of helpers for Apache Spark.
 *
 * Copyright (c) 2015 Red Hat, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.redhat.et.silex.util

trait Logging {
  // Adapted from the Logging trait in Spark 1.x
  import org.slf4j.{Logger, LoggerFactory}
  
  @transient private var _logger: Logger = null
  
  def logger: Logger = {
    if (_logger != null) {
      _logger
    } else {
      _logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
      _logger
    }
  }
  
  protected def logInfo(msg: => String) {
    if (logger.isInfoEnabled) logger.info(msg)
  }

  protected def logDebug(msg: => String) {
    if (logger.isDebugEnabled) logger.debug(msg)
  }

  protected def logWarning(msg: => String) {
    if (logger.isWarnEnabled) logger.warn(msg)
  }

  protected def logError(msg: => String) {
    if (logger.isErrorEnabled) logger.error(msg)
  }
  
}