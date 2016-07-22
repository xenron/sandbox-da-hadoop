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

import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -size expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Size extends NumberExpression {
  private static final String[] USAGE = {
    "-size n[c]"
  };
  private static final String[] HELP = {
    "Evaluates to true if the file size in 512 byte blocks is n.",
    "If n is followed by the character 'c' then the size is in bytes."
  };

  public Size() {
    setUsage(USAGE);
    setHelp(HELP);
  }

  @Override
  protected void parseArgument(String arg) throws IOException {
    if(arg.endsWith("c")) {
      arg = arg.substring(0, arg.length() - 1);
    }
    else {
      setUnits(512);
    }
    super.parseArgument(arg);
  }
  
  /** {@inheritDoc} */
  @Override
  public Result apply(PathData item) throws IOException {
    return applyNumber(getFileStatus(item).getLen());
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Size.class, "-size");
  }
}
