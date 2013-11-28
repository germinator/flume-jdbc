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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * A wrapper to manage JDBC connections. This is just a place to keep the
 * connection management code out of the abstract sink class. Trying to keep
 * things cleaner.
 */
public class JDBCConnectionManager implements Configurable {

  private static final Logger LOG = LoggerFactory
      .getLogger(JDBCConnectionManager.class);
  private static final int CONNECTION_VALID_TIMEOUT = 500;
  private String driver;
  private String url;
  private String user;
  private String password;
  private Connection connection;
  private SinkCounter counter;

  public JDBCConnectionManager(final SinkCounter counter) {
    this.counter = counter;
  }

  @Override
  public void configure(final Context context) {
    driver = context.getString("driver");
    Preconditions.checkNotNull(driver, "Driver must be specified.");
    url = context.getString("url");
    Preconditions.checkNotNull(url, "URL must be specified.");
    user = context.getString("user");
    Preconditions.checkNotNull(user, "User must be specified.");
    password = context.getString("password");
    Preconditions.checkNotNull(password, "Driver must be specified.");
  }

  public Connection getConnection() {
    return connection;
  }

  /**
   * Start the connection manager: load the driver and create a connection. Note
   * that the sink counter must have been started before calling this method.
   */
  public void start() {
    try {
      Class.forName(driver);
      createConnection();
    } catch (final Exception e) {
      LOG.error("Unable to create JDBC connection to {}.", url, e);
      counter.incrementConnectionFailedCount();
      closeConnection();
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Ensure that the current JDBC connection is valid. If it's not, create one.
   * 
   * @throws SQLException
   * @throws ClassNotFoundException
   */
  public void ensureConnectionValid() throws SQLException,
      ClassNotFoundException {
    LOG.debug("Testing JDBC connection validity to: {}.", url);
    if (connection == null || !connection.isValid(CONNECTION_VALID_TIMEOUT)) {
      closeConnection();
      createConnection();
    }
  }

  /**
   * Closes the current JDBC connection.
   */
  public void closeConnection() {
    if (connection != null) {
      LOG.debug("Closing JDBC connection to: {}.", url);

      try {
        connection.close();
      } catch (final SQLException e) {
        LOG.warn("Unable to close JDBC connection to {}.", url, e);
      }

      connection = null;
      counter.incrementConnectionClosedCount();
    }
  }

  private void createConnection() throws ClassNotFoundException, SQLException {
    LOG.debug("Creating JDBC connection to: {}.", url);
    connection = DriverManager.getConnection(url, user, password);
    connection.setAutoCommit(false);
    counter.incrementConnectionCreatedCount();
  }

}
