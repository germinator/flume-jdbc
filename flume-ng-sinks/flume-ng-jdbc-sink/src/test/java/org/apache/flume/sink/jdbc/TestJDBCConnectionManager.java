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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.hamcrest.CoreMatchers.*;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.flume.Context;
import org.apache.flume.instrumentation.SinkCounter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class TestJDBCConnectionManager {

  private static final String DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
  private static final String URL = "jdbc:derby:memory:test;create=true";
  private static final String USER = "user";
  private static final String PASSWORD = "password";
  private JDBCConnectionManager manager;
  private SinkCounter counter;

  @Before
  public void setUp() {
    counter = mock(SinkCounter.class);
    manager = new JDBCConnectionManager(counter);
  }

  @After
  public void tearDown() {
    manager.closeConnection();
  }

  @Test(expected = NullPointerException.class)
  public void testConfigureRequiredDriver() {
    manager.configure(newContext(null, "url", "user", "password"));
  }

  @Test(expected = NullPointerException.class)
  public void testConfigureRequiredURL() {
    manager.configure(newContext("driver", null, "user", "password"));
  }

  @Test(expected = NullPointerException.class)
  public void testConfigureRequiredUser() {
    manager.configure(newContext("driver", "url", null, "password"));
  }

  @Test(expected = NullPointerException.class)
  public void testConfigureRequiredPassword() {
    manager.configure(newContext("driver", "url", "user", null));
  }

  @Test
  public void testStartup() throws SQLException {
    manager.configure(newContext(DRIVER, URL, USER, PASSWORD));
    manager.start();
    assertTrue(manager.getConnection().isValid(100));
    verify(counter).incrementConnectionCreatedCount();
  }

  @Test
  public void testClose() throws SQLException {
    manager.configure(newContext(DRIVER, URL, USER, PASSWORD));
    manager.start();
    manager.closeConnection();
    assertThat(manager.getConnection(), nullValue());
    verify(counter).incrementConnectionClosedCount();
  }

  @Test
  public void testEnsure() throws SQLException, ClassNotFoundException {
    manager.configure(newContext(DRIVER, URL, USER, PASSWORD));
    manager.start();
    Connection c = manager.getConnection();
    manager.ensureConnectionValid();
    assertSame(manager.getConnection(), c);
    manager.closeConnection();
    manager.ensureConnectionValid();
    assertNotSame(manager.getConnection(), c);
    assertTrue(c.isClosed());
    assertTrue(manager.getConnection().isValid(100));
  }

  private static Context newContext(final String driver, final String url,
      final String user, final String password) {
    final ImmutableMap.Builder<String, String> b = ImmutableMap
        .<String, String> builder();
    put(b, "driver", driver);
    put(b, "url", url);
    put(b, "user", user);
    put(b, "password", password);
    return new Context(b.build());
  }

  private static void put(final ImmutableMap.Builder<String, String> b,
      final String k, final String v) {
    if (v != null) {
      b.put(k, v);
    }
  }
}
