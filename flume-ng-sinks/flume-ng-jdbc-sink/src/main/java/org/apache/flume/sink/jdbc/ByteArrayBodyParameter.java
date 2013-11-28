/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.jdbc;

import java.sql.PreparedStatement;

import org.apache.flume.Event;

/**
 * A parameter that puts the byte array header into a the database as.
 */
public class ByteArrayBodyParameter extends BodyParameter {

  public ByteArrayBodyParameter(final int id) {
    super(id);
  }

  @Override
  public void setValue(final PreparedStatement ps, final Event e)
      throws Exception {
    // Null bodies are not valid. They are always at least arrays of zero
    // length.
    ps.setBytes(id, e.getBody());
  }

}
