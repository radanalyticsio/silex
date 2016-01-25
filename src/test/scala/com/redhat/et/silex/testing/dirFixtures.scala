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

package com.redhat.et.silex.testing

import org.scalatest._

import com.redhat.et.silex.app.TestConsoleApp

import java.nio.file.{Path, Files}


trait TempDirFixtures extends BeforeAndAfterEach {
  self: BeforeAndAfterEach with Suite =>
  
  private var tempDir: Option[Path] = None
  
  override def beforeEach() {
    tempDir = Some(Files.createTempDirectory("silex-test"))
    super.beforeEach()
  }
  
  override def afterEach() {
    tempDir.map { p => Files.deleteIfExists(p) }
    super.afterEach()
  }
}