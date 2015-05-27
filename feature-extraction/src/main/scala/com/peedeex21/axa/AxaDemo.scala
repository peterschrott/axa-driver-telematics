package com.peedeex21.axa

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._

object AxaDemo {
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }


    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get input data
    val data: DataSet[(Double, Double)] = env.readCsvFile(inputPath)

    val distance = data.map(x => {

      });

    // emit result
    distance.print()

    // execute programdist
    env.execute("Axa Demo")
  }

  final class FileToRowMapper extends RichMapFunction[(Double, Double), (Double, Double, String)] {

    def map(row: (Double, Double)): (Double, Double, String) = {
      (1,1,"")
    }

  }


  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 2) {
      inputPath = args(0)
      outputPath = args(1)
      true
    } else {
      System.out.println("Executing AxaDemo.")
      System.out.println("  Provide parameters to read input data from a file.")
      System.out.println("  Usage: AxaDemo <input path> <result path>")
      false
    }
  }

  private var inputPath: String = null
  private var outputPath: String = null

}
