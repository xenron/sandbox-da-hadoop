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
package usingflume.ch03;

import org.apache.flume.serialization.ResettableInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class ResettableProtobufInputStream extends ResettableInputStream {

  private ByteArrayInputStream stream;

  public ResettableProtobufInputStream(ByteArrayInputStream stream) {
    this.stream = stream;
  }

  @Override
  public int readChar() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void mark() throws IOException {
    stream.mark(Integer.MAX_VALUE);
  }

  @Override
  public void reset() throws IOException {
    stream.reset();
  }

  @Override
  public void seek(long position) throws IOException {
    throw new UnsupportedOperationException("Unimplemented in test class");
  }

  @Override
  public long tell() throws IOException {
    throw new UnsupportedOperationException("Unimplemented in test class");
  }

  @Override
  public int read() throws IOException {
    return stream.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return stream.read(b, off, len);
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }
}