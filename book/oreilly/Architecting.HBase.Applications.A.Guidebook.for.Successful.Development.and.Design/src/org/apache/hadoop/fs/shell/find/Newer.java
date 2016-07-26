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
package org.apache.hadoop.fs.shell.find;

import java.io.IOException;
import java.util.Deque;

import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -newer expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Newer extends BaseExpression {
  private static final String[] USAGE = {
    "-newer file"
  };
  private static final String[] HELP = {
    "Evaluates as true if the modification time of the current",
    "file is more recent than the modification time of the",
    "specified file."
  };
  private long filetime;
  
  public Newer() {
    super();
    setUsage(USAGE);
    setHelp(HELP);
  }

  /** {@inheritDoc} */
  @Override
  public void initialise(FindOptions options) throws IOException{
    super.initialise(options);
    String pathname = getArgument(1);
    PathData pathData = new PathData(pathname, getConf());
    filetime = getFileStatus(pathData).getModificationTime();
  }
  
  /** {@inheritDoc} */
  @Override
  public Result apply(PathData item) throws IOException {
    if(getFileStatus(item).getModificationTime() > filetime) {
      return Result.PASS;
    }
    return Result.FAIL;
  }
  
  /** {@inheritDoc} */
  @Override
  public void addArguments(Deque<String> args) {
    addArguments(args, 1);
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Newer.class, "-newer");
  }
}
