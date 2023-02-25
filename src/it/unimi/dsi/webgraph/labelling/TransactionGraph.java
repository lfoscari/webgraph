/*
 * Copyright (C) 2011-2023 Sebastiano Vigna
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

package it.unimi.dsi.webgraph.labelling;

import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.chars.CharSets;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.objects.Object2IntFunction;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ImmutableSequentialGraph;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.labelling.ScatteredLabelledArcsASCIIGraph.LabelMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static it.unimi.dsi.fastutil.io.FastBufferedInputStream.*;
import static it.unimi.dsi.webgraph.Transform.processTransposeBatch;

public class TransactionGraph extends ImmutableSequentialGraph {
	private static final Logger LOGGER = LoggerFactory.getLogger(TransactionGraph.class);
	private final static boolean DEBUG = true;

	/**
	 * The default batch size.
	 */
	public static final int DEFAULT_BATCH_SIZE = 1000000;
	/**
	 * The default label prototype.
	 */
	public static final Label DEFAULT_LABEL_PROTOTYPE = new GammaCodedIntLabel("transaction-id");
	/**
	 * The default label mapping function.
	 */
	public static final LabelMapping DEFAULT_LABEL_MAPPING = (label, st) -> ((GammaCodedIntLabel) label).value = Integer.parseInt((String) st);
	/**
	 * The extension of the identifier file (a binary list of longs).
	 */
	private static final String IDS_EXTENSION = ".ids";
	/**
	 * TODO
	 */
	private static final char SEPARATOR = '\t';
	/**
	 * The labelled batch graph used to return node iterators.
	 */
	private final Transform.ArcLabelledBatchGraph arcLabelledBatchGraph;
	/**
	 * The list of identifiers in order of appearance.
	 */
	public long[] ids;

	public static class ReadTransactions {
		private final FastBufferedInputStream stream;
		private final Charset charset;
		private final int numNodes;
		private final Object2IntFunction<? extends CharSequence> addressMap;

		private final IntArrayList addresses = IntArrayList.of();

		private byte[] previousTransaction;
		private byte[] nextTransaction = null;
		private int nextAddressId = -1;

		private int lineLength;
		private int offset;
		private byte[] array = new byte[1024];
		private int line = 1;

		public ReadTransactions(FastBufferedInputStream stream, final Charset charset, final int numNodes, final Object2IntFunction<? extends CharSequence> addressMap) {
			this.stream = stream;
			this.charset = charset;
			this.numNodes = numNodes;
			this.addressMap = addressMap;
		}

		public IntArrayList nextAddresses() throws IOException {
			addresses.clear();
			byte[] transaction = null;

			if (nextTransaction != null) {
				transaction = nextTransaction;
				addresses.add(nextAddressId);
				nextTransaction = null;
				nextAddressId = -1;
			}

			for (;; line++) {
				offset = 0;

				// Skip until you find a non-empty line
				do {
					if ((lineLength = lineLength()) == -1) {
						return addresses; // EOF
					}
				} while (skipWhitespace() == -1);

				// Scan transaction.
				int start = offset;
				while(offset < lineLength && (array[offset] < 0 || array[offset] > ' ')) offset++;

				final byte[] currentTransaction = Arrays.copyOfRange(array, start, offset - start);

				if (DEBUG) {
					System.err.println("Parsed transaction at line " + line + ": " + new String(currentTransaction, charset));
				}

				// Skip whitespace between identifiers.
				skipWhitespace();

				// Scan address.
				start = offset;
				while(offset < lineLength && (array[offset] < 0 || array[offset] > ' ')) offset++;

				// TODO: switch to using the array method to get the keys from the map
				final String address = new String(array, start, offset - start, charset);
				final int addressId = addressMap.getInt(address);

				if (addressId == -1) {
					LOGGER.warn("Unknown address identifier " + address + " at line " + line);
					continue;
				}

				if (addressId < 0 || addressId >= numNodes) {
					throw new IllegalArgumentException("Address id out of range for node " + addressId + ": " + addressId);
				}

				if (DEBUG) {
					System.err.println("Parsed address at line " + line + ": " + address + " => " + addressId);
				}

				if (previousTransaction == null) {
					previousTransaction = currentTransaction;
				}

				if (transaction == null) {
					transaction = currentTransaction;
					addresses.add(addressId);
				} else if (Arrays.equals(currentTransaction, transaction)) {
					addresses.add(addressId);
				} else {
					nextTransaction = currentTransaction;
					nextAddressId = addressId;
					break;
				}
			}

			return addresses;
		}

		private int skipWhitespace() {
			while(offset < lineLength && array[offset] >= 0 && array[offset] <= ' ') offset++;
			if (offset == lineLength || array[0] == '#') return -1;
			return offset;
		}

		private int lineLength() throws IOException {
			int start = 0, len;
			while((len = stream.readLine(array, start, array.length - start, ALL_TERMINATORS)) == array.length - start) {
				start += len;
				array = ByteArrays.grow(array, array.length + 1);
			}

			if (len == -1) return -1; // EOF
			return start + len;
		}

		public byte[] transaction() {
			return previousTransaction;
		}

		public int line() {
			return line;
		}
	}

	public TransactionGraph(
			final InputStream inputsIs,
			final InputStream outputsIs,
			final Object2IntFunction<? extends CharSequence> addressMap,
			Charset charset,
			final int numNodes,
			final Label labelPrototype,
			final LabelMapping labelMapping,
			final int batchSize,
			final File tempDir,
			final ProgressLogger pl) throws IOException {

		// Inputs and outputs are in the form: <transaction> <address> and sorted by transaction.

		if (charset == null) {
			charset = StandardCharsets.ISO_8859_1;
		}

		final ReadTransactions outputs = new ReadTransactions(new FastBufferedInputStream(outputsIs), charset, numNodes, addressMap);
		final ReadTransactions inputs = new ReadTransactions(new FastBufferedInputStream(inputsIs), charset, numNodes, addressMap);

		int j = 0;
		long pairs = 0; // Number of pairs

		int[] source = new int[batchSize], target = new int[batchSize];
		long[] start = new long[batchSize];

		FastByteArrayOutputStream fbos = new FastByteArrayOutputStream();
		OutputBitStream obs = new OutputBitStream(fbos);
		final ObjectArrayList<File> batches = new ObjectArrayList<>();
		final ObjectArrayList<File> labelBatches = new ObjectArrayList<>();
		final Label prototype = labelPrototype.copy();

		if (pl != null) {
			pl.itemsName = "arcs";
			pl.start("Creating sorted batches...");
		}

		while (true) {
			final IntArrayList outputAddresses = outputs.nextAddresses();
			final IntArrayList inputAddresses = inputs.nextAddresses();

			if (outputAddresses.size() == 0) {
				break;
			}

			byte[] transaction = outputs.transaction();

			if (!Arrays.equals(inputs.transaction(), transaction)) {
				throw new RuntimeException("Inconsistency in inputs and outputs at output line " + outputs.line() + " and input line " + inputs.line());
			}

			// Set the label as the transaction
			labelMapping.apply(prototype, new String(transaction, charset));

			for (int s: inputAddresses) {
				for (int t: outputAddresses) {
					if (s == t) {
						continue;
					}

					source[j] = s;
					target[j] = t;
					start[j] = obs.writtenBits();
					prototype.toBitStream(obs, s);
					j++;

					if (j == batchSize) {
						obs.flush();
						pairs += processTransposeBatch(batchSize, source, target, start, new InputBitStream(fbos.array), tempDir, batches, labelBatches, prototype, null);
						fbos = new FastByteArrayOutputStream();
						obs = new OutputBitStream(fbos);
						j = 0;
					}

					if (pl != null) {
						pl.lightUpdate();
					}
				}
			}
		}

		if (j != 0) {
			obs.flush();
			pairs += processTransposeBatch(j, source, target, start, new InputBitStream(fbos.array), tempDir, batches, labelBatches, prototype, null);
		}

		if (pl != null) {
			pl.done();
			logBatches(batches, pairs, pl);
		}

		source = null;
		target = null;
		start = null;

		this.arcLabelledBatchGraph = new Transform.ArcLabelledBatchGraph(numNodes, pairs, batches, labelBatches, prototype, null);
	}

	protected static void logBatches(final ObjectArrayList<File> batches, final long pairs, final ProgressLogger pl) {
		long length = 0;
		for (final File f : batches) length += f.length();
		pl.logger().info("Created " + batches.size() + " batches using " + Util.format((double)Byte.SIZE * length / pairs) + " bits/arc.");
	}

	@Override
	public int numNodes() {
		if (this.arcLabelledBatchGraph == null)
			throw new UnsupportedOperationException("The number of nodes is unknown (you need to exhaust the input)");
		return this.arcLabelledBatchGraph.numNodes();
	}

	@Override
	public long numArcs() {
		if (this.arcLabelledBatchGraph == null)
			throw new UnsupportedOperationException("The number of arcs is unknown (you need to exhaust the input)");
		return this.arcLabelledBatchGraph.numArcs();
	}

	@Override
	public ArcLabelledNodeIterator nodeIterator() {
		return this.arcLabelledBatchGraph.nodeIterator(0);
	}

	@Override
	public boolean hasCopiableIterators() {
		return this.arcLabelledBatchGraph.hasCopiableIterators();
	}

	@Override
	public TransactionGraph copy() {
		return this;
	}

	@Override
	public String toString() {
		final MutableString ms = new MutableString();
		ArcLabelledNodeIterator nodeIterator = nodeIterator();
		ms.append("Addresses: " + numNodes() + "\nTransactions: " + numArcs() + "\n");
		while (nodeIterator.hasNext()) {
			int node = nodeIterator.nextInt();
			ArcLabelledNodeIterator.LabelledArcIterator successors = nodeIterator.successors();
			Label[] transactions = nodeIterator.labelArray();
			ms.append("Successors of " + node + " (degree " + nodeIterator.outdegree() + "):");
			for (int k = 0; k < nodeIterator.outdegree(); k++) {
				ms.append(" " + successors.nextInt() + " (" + transactions[k] + ")");
			}
			ms.append("\n");
		}
		return ms.toString();
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		Path resources = new File("/mnt/extra/analysis/lfoscari").toPath();
		Path artifacts = resources.resolve("artifacts");
		Path inputsFile = resources.resolve("inputs.tsv");
		Path outputsFile = resources.resolve("outputs.tsv");
		Path graphDir = resources.resolve("graph-labelled");

		GOVMinimalPerfectHashFunction<MutableString> transactionsMap = (GOVMinimalPerfectHashFunction<MutableString>)
				BinIO.loadObject(artifacts.resolve("transactions.map").toFile());
		GOVMinimalPerfectHashFunction<MutableString> addressMap = (GOVMinimalPerfectHashFunction<MutableString>)
				BinIO.loadObject(artifacts.resolve("addresses.map").toFile());
		int numNodes = (int) addressMap.size64();

		Object2IntFunction<? extends CharSequence> addressFunction = (a) -> (int) addressMap.getLong(a);
		LabelMapping labelMapping = (l, s) -> ((GammaCodedIntLabel) l).value = (int) transactionsMap.getLong(s);

		Logger logger = LoggerFactory.getLogger(TransactionGraph.class);
		ProgressLogger pl = new ProgressLogger(logger, 1, TimeUnit.MINUTES);

		TransactionGraph graph = new TransactionGraph(new FileInputStream(inputsFile.toFile()), new FileInputStream(outputsFile.toFile()),
				addressFunction, null, numNodes, DEFAULT_LABEL_PROTOTYPE, labelMapping, DEFAULT_BATCH_SIZE, null, pl);
		BVGraph.storeLabelled(graph.arcLabelledBatchGraph, graphDir.resolve("bitcoin").toString(), graphDir.resolve("bitcoin-underlying").toString());
	}
}