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
 * Implements the -mtime expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Mtime extends NumberExpression {
  private static final String[] USAGE = {
    "-mtime n",
    "-mmin n"
  };
  private static final String[] HELP = {
    "Evaluates as true if the file modification time subtracted",
    "from the start time is n days (or minutes if -mmin is used)"
  };

  public Mtime() {
    this(DAY_IN_MILLISECONDS);
  }
  public Mtime(long units) {
    super(units);
    setUsage(USAGE);
    setHelp(HELP);
  }

  /** {@inheritDoc} */
  @Override
  public Result apply(PathData item) throws IOException {
    return applyNumber(getOptions().getStartTime() - getFileStatus(item).getModificationTime());
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Mtime.class, "-mtime");
    factory.addClass(Mmin.class, "-mmin");
  }
  
  /** Implement -mmin expression (similar to -mtime but in minutes). */
  public static class Mmin extends FilterExpression {
    public Mmin() {
      super(new Mtime(MINUTE_IN_MILLISECONDS));
    }
  }
}
