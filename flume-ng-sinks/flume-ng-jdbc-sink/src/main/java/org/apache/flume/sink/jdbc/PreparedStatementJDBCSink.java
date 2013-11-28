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
import java.sql.SQLException;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PreparedStatementJDBCSink extends AbstractJDBCSink {

  private static final Logger LOG = LoggerFactory
      .getLogger(PreparedStatementJDBCSink.class);
  private String sql;
  private PreparedStatement statement;
  private PreparedStatementParser parser;

  @Override
  public void configure(final Context context) {
    super.configure(context);
    sql = context.getString("sql");
    parser = new PreparedStatementParser(sql);
  }

  @Override
  protected void prepareJDBC() throws SQLException {
    // Prepare the prepared statement.
    statement = getConnection().prepareStatement(parser.getPreparedSQL());
  }

  @Override
  protected void processJDBC(final Event event) throws Exception {
    // Set all of the parameters into the prepared statement, then add to batch.
    for (final Parameter p : parser.getParameters()) {
      p.setValue(statement, event);
    }
    statement.addBatch();
  }

  @Override
  protected void completeJDBC() throws SQLException {
    try {
      statement.executeBatch();
    } finally {
      statement.close();
    }
  }

  @Override
  protected void abortJDBC() {
    try {
      statement.close();
    } catch (final SQLException e) {
      LOG.error("Unable to properly close statement on JDBC abort.", e);
    } finally {
      statement = null;
    }
  }

}
