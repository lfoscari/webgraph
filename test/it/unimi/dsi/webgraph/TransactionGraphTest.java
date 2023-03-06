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

import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.longs.Long2IntFunction;
import it.unimi.dsi.fastutil.objects.Object2IntFunction;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.webgraph.labelling.*;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
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

	private static FastByteArrayInputStream parse(String s) {
		return new FastByteArrayInputStream(s.getBytes(StandardCharsets.US_ASCII));
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

		g = new TransactionGraph(parse("a 0\na 1\na 2"), parse("a 3"));
		assertEquals(new ArrayListMutableGraph(4, new int[][] {{0, 3}, {1, 3}, {2, 3}}).immutableView(), g);

		g = new TransactionGraph(parse("a 0"), parse("a 1"));
		assertEquals(new ArrayListMutableGraph(2, new int[][] {{0, 1}}).immutableView(), g);

		g = new TransactionGraph(parse("a 0\nb 2\nc 3"), parse("a 1"));
		assertEquals(new ArrayListMutableGraph(4, new int[][] {{0, 1}}).immutableView(), g);
	}
}
