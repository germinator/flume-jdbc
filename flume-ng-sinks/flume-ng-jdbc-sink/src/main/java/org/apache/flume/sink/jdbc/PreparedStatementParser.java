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

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * A class to parse the SQL statement provided into the config.  It does this by replacing any
 * tokens, say, "${header.foo:string}" with prepared statement parameters "?" and creating a
 * Parameter class item to manage the coversion from event to prepared statement.
 */
public class PreparedStatementParser {

	private static final Logger LOG = LoggerFactory.getLogger(PreparedStatementParser.class);
	private static final Pattern TOKEN_PATTERN = Pattern.compile("\\$\\{(.+?):(.+?)\\}");
	private static final Pattern TYPE_CONFIG_PATTERN = Pattern.compile("(.+)\\((.*?)\\)");
	private String preparedSql;
	private List<Parameter> parameters = Lists.newArrayList();
	
	public PreparedStatementParser(final String sql) {
		parse(sql);
	}
	
	public String getPreparedSQL() {
		return preparedSql;
	}

	public List<Parameter> getParameters() {
		return parameters;
	}

	private void parse(String sql) {
		LOG.debug("Parsing parameterized SQL statement: '{}'", sql);
		
		// For each token, find it, create a parameter for it, and replace it with a ?.
		Matcher matcher = TOKEN_PATTERN.matcher(sql);
		int id = 1;
		while (matcher.find()) {
			final String item = matcher.group(1);
			final String typeConfig = matcher.group(2);
			final TypeConfig tc = parseTypeConfig(typeConfig);
			parameters.add(Parameter.newParameter(id, item, tc.type, tc.config));
			sql = matcher.replaceFirst("?");
			matcher = TOKEN_PATTERN.matcher(sql);
			id++;
			LOG.debug("Replaced token with item: '{}' and type: '{}' with config: '{}'", new Object[] { item, tc.type, tc.config });
		}
		
		preparedSql = sql;
		LOG.debug("Resulting parameterized SQL statement: '{}'.", preparedSql);
	}
	
	private static TypeConfig parseTypeConfig(String typeConfig) {
		final Matcher m = TYPE_CONFIG_PATTERN.matcher(typeConfig);
		if (!m.matches()) {
			return new TypeConfig(typeConfig, null);
		}
		return new TypeConfig(m.group(1), m.group(2));
	}
	
	private static class TypeConfig {
		public final String type;
		public final String config;
		public TypeConfig(final String t, final String c) {
			type = t;
			config = c;
		}
	}

}
