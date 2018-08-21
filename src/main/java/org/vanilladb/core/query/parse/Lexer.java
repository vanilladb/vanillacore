/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.query.parse;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;

/**
 * The lexical analyzer.
 */
public class Lexer {
	private Collection<String> keywords;
	private StreamTokenizer tok;

	/**
	 * Creates a new lexical analyzer for the specified SQL statement.
	 * 
	 * @param s
	 *            the SQL statement
	 */
	public Lexer(String s) {
		initKeywords();
		tok = new StreamTokenizer(new StringReader(s));
		tok.wordChars('_', '_');
		tok.ordinaryChar('.');
		/*
		 * Tokens in TT_WORD type like ids and keywords are converted into lower
		 * case.
		 */
		tok.lowerCaseMode(true);
		nextToken();
	}

	/*
	 * Methods to check the status of the current token.
	 */

	/**
	 * Returns true if the current token is the specified delimiter character.
	 * 
	 * @param delimiter
	 *            a character denoting the delimiter
	 * @return true if the delimiter is the current token
	 */
	public boolean matchDelim(char delimiter) {
		return delimiter == (char) tok.ttype;
	}

	/**
	 * Returns true if the current token is a numeric value.
	 * 
	 * @return true if the current token is a numeric value
	 */
	public boolean matchNumericConstant() {
		return tok.ttype == StreamTokenizer.TT_NUMBER;
	}

	/**
	 * Returns true if the current token is a string.
	 * 
	 * @return true if the current token is a string
	 */
	public boolean matchStringConstant() {
		return '\'' == (char) tok.ttype;
	}

	/**
	 * Returns true if the current token is the specified keyword.
	 * 
	 * @param keyword
	 *            the keyword string
	 * @return true if that keyword is the current token
	 */
	public boolean matchKeyword(String keyword) {
		return tok.ttype == StreamTokenizer.TT_WORD && tok.sval.equals(keyword)
				&& keywords.contains(tok.sval);
	}

	/**
	 * Returns true if the current token is a legal identifier.
	 * 
	 * @return true if the current token is an identifier
	 */
	public boolean matchId() {
		return tok.ttype == StreamTokenizer.TT_WORD
				&& !keywords.contains(tok.sval);
	}

	/*
	 * Methods to "eat" the current token.
	 */

	/**
	 * Throws an exception if the current token is not the specified delimiter.
	 * Otherwise, moves to the next token.
	 * 
	 * @param delimiter
	 *            a character denoting the delimiter
	 */
	public void eatDelim(char delimiter) {
		if (!matchDelim(delimiter))
			throw new BadSyntaxException();
		nextToken();
	}

	/**
	 * Throws an exception if the current token is not an integer. Otherwise,
	 * returns that integer and moves to the next token.
	 * 
	 * @return the integer value of the current token
	 */
	public double eatNumericConstant() {
		if (!matchNumericConstant())
			throw new BadSyntaxException();
		double d = tok.nval;
		nextToken();
		return d;
	}

	/**
	 * Throws an exception if the current token is not a string. Otherwise,
	 * returns that string and moves to the next token.
	 * 
	 * @return the string value of the current token
	 */
	public String eatStringConstant() {
		if (!matchStringConstant())
			throw new BadSyntaxException();
		/*
		 * The input string constant is a quoted string token likes 'str', and
		 * its token type (ttype) is the quote character. So the string
		 * constants are not converted to lower case.
		 */
		String s = tok.sval;
		nextToken();
		return s;
	}

	/**
	 * Throws an exception if the current token is not the specified keyword.
	 * Otherwise, moves to the next token.
	 * 
	 * @param keyword
	 *            the keyword string
	 */
	public void eatKeyword(String keyword) {
		if (!matchKeyword(keyword))
			throw new BadSyntaxException();
		nextToken();
	}

	/**
	 * Throws an exception if the current token is not an identifier. Otherwise,
	 * returns the identifier string and moves to the next token.
	 * 
	 * @return the string value of the current token
	 */
	public String eatId() {
		if (!matchId())
			throw new BadSyntaxException();
		String s = tok.sval;
		nextToken();
		return s;
	}

	private void nextToken() {
		try {
			tok.nextToken();
		} catch (IOException e) {
			throw new BadSyntaxException();
		}
	}

	private void initKeywords() {
		keywords = Arrays.asList("select", "from", "where", "and", "insert",
				"into", "values", "delete", "drop", "update", "set", "create", "table",
				"int", "double", "varchar", "view", "as", "index", "on",
				"long", "order", "by", "asc", "desc", "sum", "count", "avg",
				"min", "max", "distinct", "group", "add", "sub", "mul", "div",
				"explain", "using", "hash", "btree");
	}
}
