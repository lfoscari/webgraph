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
import it.unimi.dsi.fastutil.longs.Long2IntFunction;
import it.unimi.dsi.fastutil.objects.*;
import it.unimi.dsi.webgraph.labelling.*;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static it.unimi.dsi.webgraph.labelling.ScatteredLabelledArcsASCIIGraph.LabelMapping;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TransactionGraphTest extends WebGraphTestCase {
	private static final Label gammaPrototype = new GammaCodedIntLabel("FOO");
	private static final Long2IntFunction identity = Math::toIntExact;
	private static final Object2IntFunction<byte[]> addressMap = (a) -> Integer.parseInt(new String((byte[]) a));
	private static final LabelMapping hashcodeMapping = (label, st) -> ((GammaCodedIntLabel)label).value = Arrays.hashCode(st);
	private static final LabelMapping integerMapping = (label, st) -> ((GammaCodedIntLabel)label).value = Integer.parseInt(new String(st));

	private static FastByteArrayInputStream str2fbais(String s) {
		return new FastByteArrayInputStream(s.getBytes(StandardCharsets.US_ASCII));
	}

	private static FastBufferedInputStream str2fbis(final String x) {
		return new FastBufferedInputStream(new ByteArrayInputStream(x.getBytes()));
	}

	private static Iterator<long[]> toArcsIterator(final String s) {
		final String[] arcs = s.split("\n");
		final List<long[]> arcSet = new ArrayList<>();
		for (final String arc : arcs) {
			final String[] parts = arc.split(" ");
			arcSet.add(new long[] {Long.parseLong(parts[0]), Long.parseLong(parts[1])});
		}
		return arcSet.iterator();
	}

	private static Iterator<Label> toLabelIterator(final String s, LabelMapping mapping) {
		Label copy = TransactionGraphTest.gammaPrototype.copy();
		final String[] labels = s.split(" ");
		final List<Label> labelSet = new ArrayList<>();
		for (final String label : labels) {
			mapping.apply(copy, label.getBytes());
			labelSet.add(copy.copy());
		}
		return labelSet.iterator();
	}

	private static int[][] getLabelValues(final ScatteredLabelledArcsASCIIGraph g) {
		int[][] labelValues = new int[g.numNodes()][];
		ArcLabelledNodeIterator it = g.nodeIterator();
		for (int i = 0; i < g.numNodes(); i++) {
			it.nextInt();
			Label[] labels = it.labelArray();
			labelValues[i] = Arrays.stream(labels).mapToInt(Label::getInt).toArray();
		}
		return labelValues;
	}

	private static class MergeIntegers implements LabelMergeStrategy {
		private final Label prototype;

		public MergeIntegers(Label prototype) {
			this.prototype = prototype;
		}

		@Override
		public Label merge(final Label first, final Label second) {
			((GammaCodedIntLabel) prototype).value = first.getInt() + second.getInt();
			return prototype;
		}
	}

	@Test
	public void testConstructor() throws IOException {
		TransactionGraph g;

		g = new TransactionGraph(str2fbais("a 0\na 1\na 2"), str2fbais("a 3"));
		assertEquals(new ArrayListMutableGraph(4, new int[][] {{0, 3}, {1, 3}, {2, 3}}).immutableView(), g);
		assertArrayEquals(new long[] {0, 1, 2, 3}, g.addresses);

		g = new TransactionGraph(str2fbais("a 0"), str2fbais("a 1"));
		assertEquals(new ArrayListMutableGraph(2, new int[][] {{0, 1}}).immutableView(), g);
		assertArrayEquals(new long[] {0, 1}, g.addresses);

		g = new TransactionGraph(str2fbais("a 0\nb 2\nc 3"), str2fbais("a 1"));
		assertEquals(new ArrayListMutableGraph(4, new int[][] {{0, 1}}).immutableView(), g);
		assertArrayEquals(new long[] {0, 1, 2, 3}, g.addresses);


		g = new TransactionGraph(str2fbais("a 0\na 1\na 2"), str2fbais("a 3"), (bb) -> Integer.parseInt(new String((byte[]) bb)), 4);
		assertEquals(new ArrayListMutableGraph(4, new int[][] {{0, 3}, {1, 3}, {2, 3}}).immutableView(), g);

		g = new TransactionGraph(str2fbais("a 0"), str2fbais("a 1"), (bb) -> Integer.parseInt(new String((byte[]) bb)), 2);
		assertEquals(new ArrayListMutableGraph(2, new int[][] {{0, 1}}).immutableView(), g);

		g = new TransactionGraph(str2fbais("a 0\nb 2\nc 3"), str2fbais("a 1"), (bb) -> Integer.parseInt(new String((byte[]) bb)), 4);
		assertEquals(new ArrayListMutableGraph(4, new int[][] {{0, 1}}).immutableView(), g);
	}

	@Test
	public void testConstructorMissingTransactions() throws IOException {
		TransactionGraph g;

		g = new TransactionGraph(str2fbais("a 0\na 1\na 2\nb 3"), str2fbais("a 3"));
		assertEquals(new ArrayListMutableGraph(4, new int[][] {{0, 3}, {1, 3}, {2, 3}}).immutableView(), g);
		assertArrayEquals(new long[] {0, 1, 2, 3}, g.addresses);

		g = new TransactionGraph(str2fbais("a 0"), str2fbais("b 1"));
		assertEquals(new ArrayListMutableGraph(1, new int[][] {}).immutableView(), g);
		assertArrayEquals(new long[] {0}, g.addresses);

		g = new TransactionGraph(str2fbais("a 0\nb 2\nc 3"), str2fbais("b 1"), (bb) -> Integer.parseInt(new String((byte[]) bb)), 4);
		assertEquals(new ArrayListMutableGraph(4, new int[][] {{2, 1}}).immutableView(), g);

		g = new TransactionGraph(str2fbais("a 0\na 1\na 2"), str2fbais("a 3"), (bb) -> Integer.parseInt(new String((byte[]) bb)), 4);
		assertEquals(new ArrayListMutableGraph(4, new int[][] {{0, 3}, {1, 3}, {2, 3}}).immutableView(), g);

		g = new TransactionGraph(str2fbais("a 0"), str2fbais("a 1"), (bb) -> Integer.parseInt(new String((byte[]) bb)), 2);
		assertEquals(new ArrayListMutableGraph(2, new int[][] {{0, 1}}).immutableView(), g);

		g = new TransactionGraph(str2fbais("a 0\nb 2\nc 3"), str2fbais("a 1"), (bb) -> Integer.parseInt(new String((byte[]) bb)), 4);
		assertEquals(new ArrayListMutableGraph(4, new int[][] {{0, 1}}).immutableView(), g);
	}

	@Test
	public void testConstructorWithStrings() throws IOException {
		final ImmutableGraph gg = ArrayListMutableGraph.newCompleteGraph(3, false).immutableView();
		final Object2IntFunction<byte[]> map = new Object2IntOpenCustomHashMap<>(ByteArrays.HASH_STRATEGY);
		map.defaultReturnValue(-1);

		map.clear();
		map.put("0".getBytes(), 0);
		map.put("1".getBytes(), 1);
		map.put("2".getBytes(), 2);
		assertEquals(gg, new TransactionGraph(str2fbais("a 0\nb 1\nc 2"), str2fbais("a 1\na 2\nb 0\nb 2\nc 0\nc 1"), map, 3));

		map.put("-1".getBytes(), 1);
		map.put("-2".getBytes(), 2);
		assertEquals(gg, new TransactionGraph(str2fbais("a 0\nb -1\nc -2"), str2fbais("a -1\na 2\nb 0\nb 2\nc 0\nc 1"), map, 3));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testTargetOutOfRange() throws IOException {
		final Object2IntFunction<byte[]> map = new Object2IntArrayMap<>();
		map.defaultReturnValue(-1);
		map.put("0".getBytes(), 0);
		map.put("1".getBytes(), 1);
		map.put("2".getBytes(), 2);

		TransactionGraph g = new TransactionGraph(str2fbais("a 0\na 1\na 2"), str2fbais("a 3"), map, 2);
		assertEquals(new ArrayListMutableGraph(4, new int[][] {{0, 3}, {1, 3}, {2, 3}}).immutableView(), g);
	}

	@Test(expected = RuntimeException.class)
	public void testEmptyStream() throws IOException {
		TransactionGraph g = new TransactionGraph(str2fbais(""), str2fbais("Io credo ch'ei credette ch'io credesse"));
		assertEquals(new ArrayListMutableGraph(0, new int[][] {}).immutableView(), new ArrayListMutableGraph(g).immutableView());
	}

	@Test(expected = RuntimeException.class)
	public void testEmptyStream2() throws IOException {
		TransactionGraph g = new TransactionGraph(str2fbais("Io credo ch'ei credette ch'io credesse"), str2fbais(""));
		assertEquals(new ArrayListMutableGraph(0, new int[][] {}).immutableView(), new ArrayListMutableGraph(g).immutableView());
	}

	@Test
	public void readTransactionOneLineTest() throws IOException {
		TransactionGraph.ReadTransactions in = new TransactionGraph.ReadTransactions(str2fbis("a 0\nb 1\nc 2"), addressMap, Integer.MAX_VALUE, null);

		assertArrayEquals(new int[] {0}, in.nextAddresses().toIntArray());
		assertEquals("a", in.transaction(Charset.defaultCharset()));

		assertArrayEquals(new int[] {1}, in.nextAddresses().toIntArray());
		assertEquals("b", in.transaction(Charset.defaultCharset()));

		assertArrayEquals(new int[] {2}, in.nextAddresses().toIntArray());
		assertEquals("c", in.transaction(Charset.defaultCharset()));
	}

	@Test
	public void readTransactionPairOneLineTest() throws IOException {
		TransactionGraph.ReadTransactions a = new TransactionGraph.ReadTransactions(str2fbis("a 0"), addressMap);
		TransactionGraph.ReadTransactions b = new TransactionGraph.ReadTransactions(str2fbis("a 1"), addressMap);

		assertArrayEquals(new int[] {0}, a.nextAddresses().toIntArray());
		assertEquals("a", a.transaction(Charset.defaultCharset()));

		assertArrayEquals(new int[] {1}, b.nextAddresses(a.currentLine, a.transactionStart, a.transactionEnd).toIntArray());
		assertEquals("a", b.transaction(Charset.defaultCharset()));
	}

	@Test
	public void readTransactionPairMultipleLines() throws IOException {
		TransactionGraph.ReadTransactions a = new TransactionGraph.ReadTransactions(str2fbis("a 0\nb 0\n c 0"), addressMap);
		TransactionGraph.ReadTransactions b = new TransactionGraph.ReadTransactions(str2fbis("c 1"), addressMap);

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
		TransactionGraph.ReadTransactions a = new TransactionGraph.ReadTransactions(str2fbis("c 1"), addressMap);
		TransactionGraph.ReadTransactions b = new TransactionGraph.ReadTransactions(str2fbis("a 0\nb 0\n c 0"), addressMap);

		assertArrayEquals(new int[] {1}, a.nextAddresses().toIntArray());
		assertEquals("c", a.transaction(Charset.defaultCharset()));

		assertArrayEquals(new int[] {0}, b.nextAddresses(a.currentLine, a.transactionStart, a.transactionEnd).toIntArray());
		assertEquals("c", b.transaction(Charset.defaultCharset()));
	}

	@Test
	public void readTransactionMultipleLinesMultipleAddresses() throws IOException {
		TransactionGraph.ReadTransactions a = new TransactionGraph.ReadTransactions(str2fbis("a 0\na 1\n a 2"), addressMap);

		assertArrayEquals(new int[] {0, 1, 2}, a.nextAddresses().toIntArray());
		assertEquals("a", a.transaction(Charset.defaultCharset()));
	}

	@Test
	public void readTransactionPairMultipleLinesMultipleAddresses() throws IOException {
		TransactionGraph.ReadTransactions a = new TransactionGraph.ReadTransactions(str2fbis("a 0\na 1\n a 2"), addressMap);
		TransactionGraph.ReadTransactions b = new TransactionGraph.ReadTransactions(str2fbis("a 0\na 3"), addressMap);

		assertArrayEquals(new int[] {0, 1, 2}, a.nextAddresses().toIntArray());
		assertEquals("a", a.transaction(Charset.defaultCharset()));

		assertArrayEquals(new int[] {0, 3}, b.nextAddresses(a.currentLine, a.transactionStart, a.transactionEnd).toIntArray());
		assertEquals("a", b.transaction(Charset.defaultCharset()));
	}

	@Test
	public void readTransactionPairMultipleLinesMultipleAddressesSkipping() throws IOException {
		TransactionGraph.ReadTransactions a = new TransactionGraph.ReadTransactions(str2fbis("b 0\nb 1\n b 2"), addressMap);
		TransactionGraph.ReadTransactions b = new TransactionGraph.ReadTransactions(str2fbis("a 0\na 0\na 0\nb 0\nb 3"), addressMap);

		assertArrayEquals(new int[] {0, 1, 2}, a.nextAddresses().toIntArray());
		assertEquals("b", a.transaction(Charset.defaultCharset()));

		assertArrayEquals(new int[] {0, 3}, b.nextAddresses(a.currentLine, a.transactionStart, a.transactionEnd).toIntArray());
		assertEquals("b", b.transaction(Charset.defaultCharset()));
	}
}
