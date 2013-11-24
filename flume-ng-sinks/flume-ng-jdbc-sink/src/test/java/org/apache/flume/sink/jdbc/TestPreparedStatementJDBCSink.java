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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class TestPreparedStatementJDBCSink {
	
	private static final String DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
	private static final String URL = "jdbc:derby:memory:test;create=true";
	private static final String USER = "user";
	private static final String PASSWORD = "password";
	private PreparedStatementJDBCSink sink;
	private Connection connection;
	private Channel channel;
	
	@Before
	public void setUp() throws ClassNotFoundException, SQLException {
		connection = initializeDatabase();
		channel = createChannel();
		sink = new PreparedStatementJDBCSink();
		sink.configure(new Context(ImmutableMap.<String, String>builder().
				put("driver", DRIVER).
				put("url", URL).
				put("user", USER).
				put("password", PASSWORD).
				put("batchSize", "10").
				put("sql", "insert into test (body, ext_id) values (${body:utf8string}, ${header.id:long})").build()));
		sink.setChannel(channel);
		sink.start();
	}

	@Test
	public void testInserts() throws EventDeliveryException, SQLException {
		pushEvent("body1".getBytes(), ImmutableMap.of("id", "1234"));
		pushEvent("body2".getBytes(), ImmutableMap.<String, String>of());
		pushEvent(new byte[] { }, ImmutableMap.of("id", "5678"));
		sink.process();
		assertTrue(verifyRow(1, "body1", 1234L));
		assertTrue(verifyRow(2, "body2", null));
		assertTrue(verifyRow(3, "", 5678L));
	}
	
	private void pushEvent(final byte[] body, final Map<String, String> headers) {
	    final Transaction t = channel.getTransaction();
	    t.begin();
	    Event event = EventBuilder.withBody(body, headers);
	    channel.put(event);
	    t.commit();
	    t.close();
	}
	
	private boolean verifyRow(final long id, final String body, final Long extId) throws SQLException {
		final PreparedStatement ps = connection.prepareStatement("select body, ext_id from test where id = ?");
		ps.setLong(1, id);
		final ResultSet rs = ps.executeQuery();
		if (!rs.next()) {
			return false;
		}
		final String bodyRs = rs.getString(1);
		Long extIdRs = rs.getLong(2);
		if (rs.wasNull()) {
			extIdRs = null;
		}
		return isSame(body, bodyRs) && isSame(extId, extIdRs);
	}

	private static Connection initializeDatabase() throws ClassNotFoundException, SQLException {
		Class.forName(DRIVER);
		final Connection c = DriverManager.getConnection(URL, USER, PASSWORD);
		c.createStatement().execute(
				"create table test ("
				+ "id integer not null generated always as identity (start with 1, increment by 1),"
				+ "body varchar(256),"
				+ "ext_id bigint)");
		return c;
	}
	
	private static <T> boolean isSame(T t1, T t2) {
		if ((t1 == null) && (t2 == null)) {
			return true;
		}
		if ((t1 == null) || (t2 == null)) {
			return false;
		}
		return t1.equals(t2);
	}
	
	private static Channel createChannel() {
		    final Channel c = new MemoryChannel();
		    Configurables.configure(c, new Context());
		    return c;
	}

	
}
