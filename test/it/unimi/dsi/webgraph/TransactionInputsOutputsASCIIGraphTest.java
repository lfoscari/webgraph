/*
 * Copyright (C) 2007-2023 Sebastiano Vigna
 *
 * This program and the accompanying materials are made available under the
 * terms of the GNU Lesser General Public License v2.1 or later,
 * which is available at
 * http://www.gnu.org/licenses/old-licenses/lgpl-2.1-standalone.html,
 * or the Apache Software License 2.0, which is available at
 * https://www.apache.org/licenses/LICENSE-2.0.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later OR Apache-2.0
 */

package it.unimi.dsi.webgraph;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.objects.*;
import it.unimi.dsi.webgraph.labelling.*;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static it.unimi.dsi.webgraph.labelling.ScatteredLabelledArcsASCIIGraph.LabelMapping;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TransactionInputsOutputsASCIIGraphTest extends WebGraphTestCase {
	private static final Object2LongFunction<byte[]> ADDRESS_MAP = (a) -> Long.parseLong(new String((byte[]) a));

	private static FastByteArrayInputStream str2fbais(String s) {
		return new FastByteArrayInputStream(s.getBytes(StandardCharsets.US_ASCII));
	}

	private static FastBufferedInputStream str2fbis(final String x) {
		return new FastBufferedInputStream(new ByteArrayInputStream(x.getBytes()));
	}

	@Test
	public void testConstructor() throws IOException {
		TransactionInputsOutputsASCIIGraph g;

		g = new TransactionInputsOutputsASCIIGraph(str2fbais("a 0\na 1\na 2"), str2fbais("a 3"));
		assertEquals(new ArrayListMutableGraph(4, new int[][] {{0, 3}, {1, 3}, {2, 3}}).immutableView(), g);
		assertArrayEquals(new long[] {0, 1, 2, 3}, g.addresses);

		g = new TransactionInputsOutputsASCIIGraph(str2fbais("a 0"), str2fbais("a 1"));
		assertEquals(new ArrayListMutableGraph(2, new int[][] {{0, 1}}).immutableView(), g);
		assertArrayEquals(new long[] {0, 1}, g.addresses);

		g = new TransactionInputsOutputsASCIIGraph(str2fbais("a 0\nb 2\nc 3"), str2fbais("a 1"));
		assertEquals(new ArrayListMutableGraph(4, new int[][] {{0, 1}}).immutableView(), g);
		assertArrayEquals(new long[] {0, 1, 2, 3}, g.addresses);


		g = new TransactionInputsOutputsASCIIGraph(str2fbais("a 0\na 1\na 2"), str2fbais("a 3"), (bb) -> Integer.parseInt(new String((byte[]) bb)), 4);
		assertEquals(new ArrayListMutableGraph(4, new int[][] {{0, 3}, {1, 3}, {2, 3}}).immutableView(), g);

		g = new TransactionInputsOutputsASCIIGraph(str2fbais("a 0"), str2fbais("a 1"), (bb) -> Integer.parseInt(new String((byte[]) bb)), 2);
		assertEquals(new ArrayListMutableGraph(2, new int[][] {{0, 1}}).immutableView(), g);

		g = new TransactionInputsOutputsASCIIGraph(str2fbais("a 0\nb 2\nc 3"), str2fbais("a 1"), (bb) -> Integer.parseInt(new String((byte[]) bb)), 4);
		assertEquals(new ArrayListMutableGraph(4, new int[][] {{0, 1}}).immutableView(), g);
	}

	@Test
	public void testConstructorMissingTransactions() throws IOException {
		TransactionInputsOutputsASCIIGraph g;

		g = new TransactionInputsOutputsASCIIGraph(str2fbais("a 0\na 1\na 2\nb 3"), str2fbais("a 3"));
		assertEquals(new ArrayListMutableGraph(4, new int[][] {{0, 3}, {1, 3}, {2, 3}}).immutableView(), g);
		assertArrayEquals(new long[] {0, 1, 2, 3}, g.addresses);

		g = new TransactionInputsOutputsASCIIGraph(str2fbais("a 0"), str2fbais("b 1"));
		assertEquals(new ArrayListMutableGraph(1, new int[][] {}).immutableView(), g);
		assertArrayEquals(new long[] {0}, g.addresses);

		g = new TransactionInputsOutputsASCIIGraph(str2fbais("a 0\nb 2\nc 3"), str2fbais("b 1"), (bb) -> Integer.parseInt(new String((byte[]) bb)), 4);
		assertEquals(new ArrayListMutableGraph(4, new int[][] {{2, 1}}).immutableView(), g);

		g = new TransactionInputsOutputsASCIIGraph(str2fbais("a 0\na 1\na 2"), str2fbais("a 3"), (bb) -> Integer.parseInt(new String((byte[]) bb)), 4);
		assertEquals(new ArrayListMutableGraph(4, new int[][] {{0, 3}, {1, 3}, {2, 3}}).immutableView(), g);

		g = new TransactionInputsOutputsASCIIGraph(str2fbais("a 0"), str2fbais("a 1"), (bb) -> Integer.parseInt(new String((byte[]) bb)), 2);
		assertEquals(new ArrayListMutableGraph(2, new int[][] {{0, 1}}).immutableView(), g);

		g = new TransactionInputsOutputsASCIIGraph(str2fbais("a 0\nb 2\nc 3"), str2fbais("a 1"), (bb) -> Integer.parseInt(new String((byte[]) bb)), 4);
		assertEquals(new ArrayListMutableGraph(4, new int[][] {{0, 1}}).immutableView(), g);
	}

	@Test
	public void testConstructorWithStrings() throws IOException {
		final ImmutableGraph gg = ArrayListMutableGraph.newCompleteGraph(3, false).immutableView();
		final Object2LongFunction<byte[]> map = new Object2LongOpenCustomHashMap<>(ByteArrays.HASH_STRATEGY);
		map.defaultReturnValue(-1);

		map.clear();
		map.put("0".getBytes(), 0);
		map.put("1".getBytes(), 1);
		map.put("2".getBytes(), 2);
		assertEquals(gg, new TransactionInputsOutputsASCIIGraph(str2fbais("a 0\nb 1\nc 2"), str2fbais("a 1\na 2\nb 0\nb 2\nc 0\nc 1"), map, 3));

		map.put("-1".getBytes(), 1);
		map.put("-2".getBytes(), 2);
		assertEquals(gg, new TransactionInputsOutputsASCIIGraph(str2fbais("a 0\nb -1\nc -2"), str2fbais("a -1\na 2\nb 0\nb 2\nc 0\nc 1"), map, 3));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testTargetOutOfRange() throws IOException {
		final Object2LongFunction<byte[]> map = new Object2LongArrayMap<>();
		map.defaultReturnValue(-1);
		map.put("0".getBytes(), 0);
		map.put("1".getBytes(), 1);
		map.put("2".getBytes(), 2);

		TransactionInputsOutputsASCIIGraph g = new TransactionInputsOutputsASCIIGraph(str2fbais("a 0\na 1\na 2"), str2fbais("a 3"), map, 2);
		assertEquals(new ArrayListMutableGraph(4, new int[][] {{0, 3}, {1, 3}, {2, 3}}).immutableView(), g);
	}

	@Test(expected = RuntimeException.class)
	public void testEmptyStream() throws IOException {
		TransactionInputsOutputsASCIIGraph g = new TransactionInputsOutputsASCIIGraph(str2fbais(""), str2fbais("Io credo ch'ei credette ch'io credesse"));
		assertEquals(new ArrayListMutableGraph(0, new int[][] {}).immutableView(), new ArrayListMutableGraph(g).immutableView());
	}

	@Test(expected = RuntimeException.class)
	public void testEmptyStream2() throws IOException {
		TransactionInputsOutputsASCIIGraph g = new TransactionInputsOutputsASCIIGraph(str2fbais("Io credo ch'ei credette ch'io credesse"), str2fbais(""));
		assertEquals(new ArrayListMutableGraph(0, new int[][] {}).immutableView(), new ArrayListMutableGraph(g).immutableView());
	}

	@Test
	public void readTransactionOneLineTest() throws IOException {
		TransactionInputsOutputsASCIIGraph.ReadTransactions in = new TransactionInputsOutputsASCIIGraph.ReadTransactions(str2fbis("a 0\nb 1\nc 2"), ADDRESS_MAP, Integer.MAX_VALUE, null);

		assertArrayEquals(new int[] {0}, in.nextAddresses().toIntArray());
		assertEquals("a", in.transaction(Charset.defaultCharset()));

		assertArrayEquals(new int[] {1}, in.nextAddresses().toIntArray());
		assertEquals("b", in.transaction(Charset.defaultCharset()));

		assertArrayEquals(new int[] {2}, in.nextAddresses().toIntArray());
		assertEquals("c", in.transaction(Charset.defaultCharset()));
	}

	@Test
	public void readTransactionPairOneLineTest() throws IOException {
		TransactionInputsOutputsASCIIGraph.ReadTransactions a = new TransactionInputsOutputsASCIIGraph.ReadTransactions(str2fbis("a 0"), ADDRESS_MAP);
		TransactionInputsOutputsASCIIGraph.ReadTransactions b = new TransactionInputsOutputsASCIIGraph.ReadTransactions(str2fbis("a 1"), ADDRESS_MAP);

		assertArrayEquals(new int[] {0}, a.nextAddresses().toIntArray());
		assertEquals("a", a.transaction(Charset.defaultCharset()));

		assertArrayEquals(new int[] {1}, b.nextAddresses(a.currentLine, a.transactionStart, a.transactionEnd).toIntArray());
		assertEquals("a", b.transaction(Charset.defaultCharset()));
	}

	@Test
	public void readTransactionPairMultipleLines() throws IOException {
		TransactionInputsOutputsASCIIGraph.ReadTransactions a = new TransactionInputsOutputsASCIIGraph.ReadTransactions(str2fbis("a 0\nb 0\n c 0"), ADDRESS_MAP);
		TransactionInputsOutputsASCIIGraph.ReadTransactions b = new TransactionInputsOutputsASCIIGraph.ReadTransactions(str2fbis("c 1"), ADDRESS_MAP);

		assertArrayEquals(new int[] {0}, a.nextAddresses().toIntArray());
		assertEquals("a", a.transaction(Charset.defaultCharset()));

		assertArrayEquals(new int[] {}, b.nextAddresses(a.currentLine, a.transactionStart, a.transactionEnd).toIntArray());
		assertEquals("a", b.transaction(Charset.defaultCharset()));

		assertArrayEquals(new int[] {0}, a.nextAddresses().toIntArray());
		assertEquals("b", a.transaction(Charset.defaultCharset()));

		assertArrayEquals(new int[] {}, b.nextAddresses(a.currentLine, a.transactionStart, a.transactionEnd).toIntArray());
		assertEquals("b", b.transaction(Charset.defaultCharset()));

		assertArrayEquals(new int[] {0}, a.nextAddresses().toIntArray());
		assertEquals("c", a.transaction(Charset.defaultCharset()));

		assertArrayEquals(new int[] {1}, b.nextAddresses(a.currentLine, a.transactionStart, a.transactionEnd).toIntArray());
		assertEquals("c", b.transaction(Charset.defaultCharset()));
	}

	@Test
	public void readTransactionPairMultipleLinesInverted() throws IOException {
		TransactionInputsOutputsASCIIGraph.ReadTransactions a = new TransactionInputsOutputsASCIIGraph.ReadTransactions(str2fbis("c 1"), ADDRESS_MAP);
		TransactionInputsOutputsASCIIGraph.ReadTransactions b = new TransactionInputsOutputsASCIIGraph.ReadTransactions(str2fbis("a 0\nb 0\n c 0"), ADDRESS_MAP);

		assertArrayEquals(new int[] {1}, a.nextAddresses().toIntArray());
		assertEquals("c", a.transaction(Charset.defaultCharset()));

		assertArrayEquals(new int[] {0}, b.nextAddresses(a.currentLine, a.transactionStart, a.transactionEnd).toIntArray());
		assertEquals("c", b.transaction(Charset.defaultCharset()));
	}

	@Test
	public void readTransactionMultipleLinesMultipleAddresses() throws IOException {
		TransactionInputsOutputsASCIIGraph.ReadTransactions a = new TransactionInputsOutputsASCIIGraph.ReadTransactions(str2fbis("a 0\na 1\n a 2"), ADDRESS_MAP);

		assertArrayEquals(new int[] {0, 1, 2}, a.nextAddresses().toIntArray());
		assertEquals("a", a.transaction(Charset.defaultCharset()));
	}

	@Test
	public void readTransactionPairMultipleLinesMultipleAddresses() throws IOException {
		TransactionInputsOutputsASCIIGraph.ReadTransactions a = new TransactionInputsOutputsASCIIGraph.ReadTransactions(str2fbis("a 0\na 1\n a 2"), ADDRESS_MAP);
		TransactionInputsOutputsASCIIGraph.ReadTransactions b = new TransactionInputsOutputsASCIIGraph.ReadTransactions(str2fbis("a 0\na 3"), ADDRESS_MAP);

		assertArrayEquals(new int[] {0, 1, 2}, a.nextAddresses().toIntArray());
		assertEquals("a", a.transaction(Charset.defaultCharset()));

		assertArrayEquals(new int[] {0, 3}, b.nextAddresses(a.currentLine, a.transactionStart, a.transactionEnd).toIntArray());
		assertEquals("a", b.transaction(Charset.defaultCharset()));
	}

	@Test
	public void readTransactionPairMultipleLinesMultipleAddressesSkipping() throws IOException {
		TransactionInputsOutputsASCIIGraph.ReadTransactions a = new TransactionInputsOutputsASCIIGraph.ReadTransactions(str2fbis("b 0\nb 1\n b 2"), ADDRESS_MAP);
		TransactionInputsOutputsASCIIGraph.ReadTransactions b = new TransactionInputsOutputsASCIIGraph.ReadTransactions(str2fbis("a 0\na 0\na 0\nb 0\nb 3"), ADDRESS_MAP);

		assertArrayEquals(new int[] {0, 1, 2}, a.nextAddresses().toIntArray());
		assertEquals("b", a.transaction(Charset.defaultCharset()));

		assertArrayEquals(new int[] {0, 3}, b.nextAddresses(a.currentLine, a.transactionStart, a.transactionEnd).toIntArray());
		assertEquals("b", b.transaction(Charset.defaultCharset()));
	}
}
