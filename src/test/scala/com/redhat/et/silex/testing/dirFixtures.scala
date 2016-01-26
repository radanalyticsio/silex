/*
 * This file is part of the "silex" library of helpers for Apache Spark.
 *
 * Copyright (c) 2016 Red Hat, Inc.
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

import java.io.File
import java.nio.file.{Path, Files}
import Files.{isSymbolicLink, createTempDirectory, deleteIfExists}

trait TempDirFixtures extends BeforeAndAfterEach {
  self: BeforeAndAfterEach with Suite =>
  
  private var tempDir: Option[File] = None
  private def tempPath: String = tempDir.map { _.toString }.get

  private [this] def cleanUp(path: File) {
    if (path.isDirectory() && !isSymbolicLink(path.toPath)) {
      path.listFiles().map { 
        case p if p.isDirectory => cleanUp(p)
        case p => p.delete()
      }
    }

    path.delete()
  }
  
  override def beforeEach() {
    tempDir = Some(createTempDirectory("silex-test").toFile)
    super.beforeEach()
  }
  
  override def afterEach() {
    tempDir.map { f => cleanUp(f) }
    super.afterEach()
  }
}
