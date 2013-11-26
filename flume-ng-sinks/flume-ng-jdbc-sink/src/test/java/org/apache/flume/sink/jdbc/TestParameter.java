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

import java.io.UnsupportedEncodingException;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.Map;

import org.apache.flume.Event;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestParameter {
	
	private PreparedStatement statement;
	private Event event;
	private Map<String, String> headers;
	
	@SuppressWarnings("unchecked")
	@Before
	public void setUp() {
		statement = mock(PreparedStatement.class);
		event = mock(Event.class);
		headers = mock(Map.class);
		when(event.getHeaders()).thenReturn(headers);
	}

	@Test
	public void testStringBodyParameterNoParameter() throws Exception {
		Parameter.newParameter(1, "body", "string", null);
	}

	@Test
	public void testStringBodyParameterBlankParameter() throws Exception {
		Parameter.newParameter(1, "body", "string", "");
	}

	@Test
	public void testStringBodyParameterValidString() throws Exception {
		Parameter p = Parameter.newParameter(1, "body", "string", "UTF-8");
		assertThat(p, notNullValue());
		when(event.getBody()).thenReturn("mystring".getBytes());
		p.setValue(statement, event);
		verify(statement).setString(1, "mystring");
	}

	@Test
	public void testStringBodyParameterEmpty() throws Exception {
		Parameter p = Parameter.newParameter(1, "body", "string", "UTF-8");
		assertThat(p, notNullValue());
		when(event.getBody()).thenReturn(new byte[] { });
		p.setValue(statement, event);
		verify(statement).setString(1, "");
	}

	@Ignore // The String constructor doesn't seem to throw this on invalid UTF-8 strings.  Maybe this is a feature?
	@Test(expected=UnsupportedEncodingException.class)
	public void testStringBodyParameterInvalidString() throws Exception {
		Parameter p = Parameter.newParameter(1, "body", "string", "UTF-8");
		assertThat(p, notNullValue());
		// Invalid UTF-8 sequence from: http://www.cl.cam.ac.uk/~mgk25/ucs/examples/UTF-8-test.txt
		when(event.getBody()).thenReturn(new byte[] { (byte) 0xfe, (byte) 0xfe, (byte) 0xff, (byte) 0xff });
		p.setValue(statement, event);
	}

	@Test
	public void testStringHeaderParameterNotNull() throws Exception {
		Parameter p = Parameter.newParameter(1, "header.foo", "string", null);
		assertThat(p, notNullValue());
		when(headers.get("foo")).thenReturn("mystring");
		p.setValue(statement, event);
		verify(statement).setString(1, "mystring");
	}

	@Test
	public void testStringHeaderParameterNull() throws Exception {
		Parameter p = Parameter.newParameter(1, "header.foo", "string", null);
		assertThat(p, notNullValue());
		when(headers.get("foo")).thenReturn(null);
		p.setValue(statement, event);
		verify(statement).setNull(1, Types.VARCHAR);
	}

	@Test
	public void testLongHeaderParameterNotNull() throws Exception {
		Parameter p = Parameter.newParameter(1, "header.foo", "long", null);
		assertThat(p, notNullValue());
		when(headers.get("foo")).thenReturn("1234");
		p.setValue(statement, event);
		verify(statement).setLong(1, 1234);
	}

	@Test
	public void testLongHeaderParameterNull() throws Exception {
		Parameter p = Parameter.newParameter(1, "header.foo", "long", null);
		assertThat(p, notNullValue());
		when(headers.get("foo")).thenReturn(null);
		p.setValue(statement, event);
		verify(statement).setNull(1, Types.BIGINT);
	}

	@Test(expected=NumberFormatException.class)
	public void testLongHeaderParameterInvalid() throws Exception {
		Parameter p = Parameter.newParameter(1, "header.foo", "long", null);
		assertThat(p, notNullValue());
		when(headers.get("foo")).thenReturn("notalong");
		p.setValue(statement, event);
	}
}
