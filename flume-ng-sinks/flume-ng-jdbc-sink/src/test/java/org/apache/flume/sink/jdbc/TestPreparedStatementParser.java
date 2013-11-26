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
import static org.hamcrest.CoreMatchers.*;

import org.junit.Test;

public class TestPreparedStatementParser {
	
	@Test
	public void testCommonStatementParsing() {
		final PreparedStatementParser p = new PreparedStatementParser("insert into mytable (mystringbody, mystringheader, mylongheader) values (${body:string(UTF-8)}, ${header.foo:string}, ${header.bar:long()})");
		assertThat(p.getPreparedSQL(), is("insert into mytable (mystringbody, mystringheader, mylongheader) values (?, ?, ?)"));
		assertThat(p.getParameters().size(), is(3));
	}

}
