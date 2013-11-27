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

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink.Status;
import org.apache.flume.Transaction;
import org.apache.flume.event.EventBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.ImmutableMap;

public class TestAbstractJDBCSink {
	
	private AbstractJDBCSink sink;
	private JDBCConnectionManager connectionManager;
	private Connection connection;
	private Channel channel;
	private Transaction transaction;
	private int counter;
	
	@Before
	public void setUp() throws Exception {
		sink = mock(AbstractJDBCSink.class, Mockito.CALLS_REAL_METHODS);
		doNothing().when(sink).prepareJDBC();
		doNothing().when(sink).processJDBC(any(Event.class));
		doNothing().when(sink).completeJDBC();
		doNothing().when(sink).abortJDBC();
		sink.configure(new Context(ImmutableMap.of("driver", "driver", "url", "url", "user", "user", "password", "password", "batchSize", "10")));
		connectionManager = mock(JDBCConnectionManager.class);
		connection = mock(Connection.class);
		when(connectionManager.getConnection()).thenReturn(connection);
		sink.setConnectionManager(connectionManager);
		channel = mock(Channel.class);
		sink.setChannel(channel);
		transaction = mock(Transaction.class);
		when(channel.getTransaction()).thenReturn(transaction);
		counter = 0;
		sink.start();
	}
	
	// Ensure that on the happy path, things seem to clean up and commit in a sensible order.
	@Test
	public void testTransactionalHappyPath() throws EventDeliveryException, SQLException, ClassNotFoundException {
		sink.process();
		final InOrder inOrder = inOrder(transaction, connectionManager, connection, sink);
		inOrder.verify(transaction).begin();
		inOrder.verify(connectionManager).ensureConnectionValid();
		inOrder.verify(sink).completeJDBC();
		inOrder.verify(connection).commit();
		verify(transaction).commit();
	}
	
	// Ensure that on some sort of failure, things seem to clean up and roll back in a sensible order.
	@Test public void testTransactionalFailurePath() throws Exception {
		when(channel.take()).thenReturn(EventBuilder.withBody("", Charset.defaultCharset()));
		doThrow(SQLException.class).when(sink).processJDBC(any(Event.class));
		try {
			sink.process();
		} catch (final Exception e) {
			assertTrue(e.getCause() instanceof SQLException);
		}
		final InOrder inOrder = inOrder(transaction, connectionManager, connection, sink);
		inOrder.verify(transaction).begin();
		inOrder.verify(connectionManager).ensureConnectionValid();
		inOrder.verify(sink).abortJDBC();
		inOrder.verify(connection).rollback();
		verify(transaction).rollback();
	}

	@Test
	public void testFullBatchStatus() throws Exception {
		when(channel.take()).thenAnswer(new Answer<Event>() {
			@Override
			public Event answer(final InvocationOnMock invocation) throws Throwable {
				if (counter < 10) {
					counter++;
					return EventBuilder.withBody(Integer.toString(counter).getBytes());
				}
				return null;
			}
		});
		assertSame(Status.READY, sink.process());
	}

	@Test
	public void testPartialBatchStatus() throws Exception {
		when(channel.take()).thenAnswer(new Answer<Event>() {
			@Override
			public Event answer(final InvocationOnMock invocation) throws Throwable {
				if (counter < 5) {
					counter++;
					return EventBuilder.withBody(Integer.toString(counter).getBytes());
				}
				return null;
			}
		});
		assertSame(Status.READY, sink.process());
	}

}
