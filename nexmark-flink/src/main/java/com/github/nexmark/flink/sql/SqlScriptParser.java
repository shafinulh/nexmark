/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.nexmark.flink.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/** Parser for SQL scripts that preserves statement sets and strips comments safely. */
final class SqlScriptParser {

	static final Pattern SET_STATEMENT_PATTERN =
			Pattern.compile(
					"SET\\s+'([^']+)'\\s*=\\s*'(.*)'\\s*;",
					Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

	private SqlScriptParser() {}

	static List<String> splitStatements(String script) {
		List<String> statements = new ArrayList<>();
		StringBuilder current = new StringBuilder();
		boolean inSingleQuote = false;
		boolean inDoubleQuote = false;
		boolean inLineComment = false;
		boolean inBlockComment = false;
		boolean inStatementSet = false;

		for (int i = 0; i < script.length(); i++) {
			char currentChar = script.charAt(i);
			char nextChar = i + 1 < script.length() ? script.charAt(i + 1) : '\0';

			if (inLineComment) {
				if (currentChar == '\n') {
					inLineComment = false;
					current.append(currentChar);
				}
				continue;
			}

			if (inBlockComment) {
				if (currentChar == '*' && nextChar == '/') {
					inBlockComment = false;
					i++;
				}
				continue;
			}

			if (!inSingleQuote && !inDoubleQuote) {
				if (currentChar == '-' && nextChar == '-') {
					inLineComment = true;
					i++;
					continue;
				}
				if (currentChar == '/' && nextChar == '*') {
					inBlockComment = true;
					i++;
					continue;
				}
			}

			current.append(currentChar);

			if (currentChar == '\'' && !inDoubleQuote) {
				if (inSingleQuote && nextChar == '\'') {
					current.append(nextChar);
					i++;
				} else {
					inSingleQuote = !inSingleQuote;
				}
				continue;
			}

			if (currentChar == '"' && !inSingleQuote) {
				if (inDoubleQuote && nextChar == '"') {
					current.append(nextChar);
					i++;
				} else {
					inDoubleQuote = !inDoubleQuote;
				}
				continue;
			}

			if (currentChar == ';' && !inSingleQuote && !inDoubleQuote) {
				String statement = current.toString().trim();
				if (statement.isEmpty()) {
					current.setLength(0);
					continue;
				}

				if (!inStatementSet && startsStatementSet(statement)) {
					inStatementSet = true;
					continue;
				}

				if (!inStatementSet || endsStatementSet(statement)) {
					statements.add(statement);
					current.setLength(0);
					inStatementSet = false;
				}
			}
		}

		String remaining = current.toString().trim();
		if (!remaining.isEmpty()) {
			statements.add(remaining);
		}
		return statements;
	}

	static boolean isTerminalDml(String statement) {
		String normalized = statement.trim().toUpperCase(Locale.ROOT);
		return normalized.startsWith("INSERT INTO")
				|| normalized.startsWith("INSERT OVERWRITE")
				|| normalized.startsWith("EXECUTE STATEMENT SET");
	}

	private static boolean startsStatementSet(String statement) {
		return statement.trim().toUpperCase(Locale.ROOT).startsWith("EXECUTE STATEMENT SET");
	}

	private static boolean endsStatementSet(String statement) {
		return statement.trim().toUpperCase(Locale.ROOT).matches("(?s).*\\bEND\\s*;\\s*$");
	}
}
