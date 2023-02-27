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
import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.objects.Object2IntFunction;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static it.unimi.dsi.fastutil.io.FastBufferedInputStream.*;
import static it.unimi.dsi.webgraph.Transform.processTransposeBatch;

public class TransactionGraph extends ImmutableSequentialGraph {
	private static final Logger LOGGER = LoggerFactory.getLogger(TransactionGraph.class);
	private final static boolean DEBUG = true;

	// Stats:
	// - # of inputs and outputs
	// - # of duplicates inputs and outputs
	// - ???

	private static class Statistics {
		private final Object2ObjectOpenHashMap<byte[], Pair<Integer, Integer>> amountInputsOutputs;
		private final Object2ObjectOpenHashMap<byte[], Pair<Integer, Integer>> duplicateInputsOutputs;

		public Statistics() {
			this(1024);
		}

		public Statistics(int transactionAmount) {
			amountInputsOutputs = new Object2ObjectOpenHashMap<>(transactionAmount);
			duplicateInputsOutputs = new Object2ObjectOpenHashMap<>(transactionAmount);
		}

		public void update(byte[] transaction, int[] inputAddresses, int[] outputAddresses) {
			amountInputsOutputs.put(transaction, Pair.of(inputAddresses.length, outputAddresses.length));

			IntArrays.quickSort(inputAddresses);
			int inputDuplicates = 0;
			for (int i = 1; i < inputAddresses.length; i++)
				if (inputAddresses[i] == inputAddresses[i - 1])
					inputDuplicates++;

			IntArrays.quickSort(outputAddresses);
			int outputDuplicates = 0;
			for (int i = 1; i < outputAddresses.length; i++)
				if (outputAddresses[i] == outputAddresses[i - 1])
					outputDuplicates++;

			duplicateInputsOutputs.put(transaction, Pair.of(inputDuplicates, outputDuplicates));
		}

		public void save(File destination) throws IOException {
			amountInputsOutputs.trim();
			BinIO.storeObject(amountInputsOutputs, destination);

			duplicateInputsOutputs.trim();
			BinIO.storeObject(duplicateInputsOutputs, destination);
		}
	}

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

	private static class ReadTransactions {
		private final FastBufferedInputStream stream;
		private final Charset charset;
		private final int numNodes;
		private final Object2IntFunction<? extends CharSequence> addressMap;

		private final IntArrayList addresses = new IntArrayList(512);

		private int lineLength;
		private int offset;
		private int line = 0;
		private int transactionStart = -1, transactionEnd = -1;
		private byte[] previousLine = new byte[1024];
		private byte[] currentLine = new byte[1024];

		public ReadTransactions(FastBufferedInputStream stream, final Charset charset, final int numNodes, final Object2IntFunction<? extends CharSequence> addressMap) throws IOException {
			this.stream = stream;
			this.charset = charset;
			this.numNodes = numNodes;
			this.addressMap = addressMap;

			do {
				lineLength = readLine(previousLine);
			} while (skipWhitespace(previousLine) == -1);
		}

		public IntArrayList nextAddresses() throws IOException {
			addresses.clear();

			if (lineLength == -1) {
				return addresses;
			}

			offset = 0;
			skipWhitespace(previousLine);

			// Scan transaction
			int start = offset;
			while(offset < lineLength && (previousLine[offset] < 0 || previousLine[offset] > ' ')) offset++;

			// Save the boundaries of the transaction in previousLine
			transactionStart = start;
			transactionEnd = offset - start;

			// Add previousLine address to address list
			offset = transactionEnd;
			addAddressFromLine(previousLine);

			for (;;) {
				// Now keep reading lines into currentLine until the transaction matches the one in previousLine
				offset = 0;

				do {
					lineLength = readLine(currentLine);
					line++;
				} while (skipWhitespace(currentLine) == -1);

				if (lineLength == -1) { // EOF
					return addresses;
				}

				// Scan transaction
				start = offset;
				while(offset < lineLength && (currentLine[offset] < 0 || currentLine[offset] > ' ')) offset++;

				// Check if transactions match
				if (!Arrays.equals(currentLine, start, offset - start, previousLine, transactionStart, transactionEnd)) {
					break;
				}

				addAddressFromLine(currentLine);
			}

			byte[] t = currentLine;
			currentLine = previousLine;
			previousLine = t;

			return addresses;
		}

		private void addAddressFromLine(final byte[] array) {
			skipWhitespace(array);

			final int start = offset;
			while(offset < lineLength && (array[offset] < 0 || array[offset] > ' ')) offset++;

			final String address = new String(array, start, offset - start, charset);
			addresses.add(addressMap.getInt(address));
		}

		private int skipWhitespace(byte[] dest) {
			while(offset < lineLength && dest[offset] >= 0 && dest[offset] <= ' ') offset++;
			if (offset == lineLength || dest[0] == '#') return -1;
			return offset;
		}

		private int readLine(byte[] dest) throws IOException {
			int start = 0, len;
			while((len = stream.readLine(dest, start, dest.length - start, ALL_TERMINATORS)) == dest.length - start) {
				start += len;
				dest = ByteArrays.grow(dest, dest.length + 1);
			}

			if (len == -1) return -1; // EOF
			return start + len;
		}

		public boolean transactionsMatch(ReadTransactions other) {
			if (transactionStart == -1) throw new IllegalStateException("You must first read addresses");
			return Arrays.equals(currentLine, transactionStart, transactionEnd, other.currentLine, other.transactionStart, other.transactionEnd);
		}

		public String transaction() {
			if (transactionStart == -1) throw new IllegalStateException("You must first read addresses");
			return new String(currentLine, transactionStart, transactionEnd, charset);
		}

		public int lineNumber() {
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

		// Inputs and outputs are in the form <transaction> <address> and sorted by transaction.

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

			if (!outputs.transactionsMatch(inputs)) {
				throw new RuntimeException("Inconsistency in inputs and outputs!\n"
						+ "\toutput [" + outputs.lineNumber() + "]:\t\"" + new String(outputs.currentLine) + "\"\n"
						+ "\tinput [" + inputs.lineNumber() + "]:\t\"" + new String(inputs.currentLine) + "\"");
			}

			// Set the label as the transaction
			labelMapping.apply(prototype, outputs.transaction());

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
		Path resources = new File(".").toPath();
		Path test = resources.resolve("transaction-test");
		Path artifacts = test;
		Path inputsFile = test.resolve("inputs.tsv");
		Path outputsFile = test.resolve("outputs.tsv");
		Path graphDir = test.resolve("graph-labelled");
		graphDir.toFile().mkdir();

		GOVMinimalPerfectHashFunction<MutableString> transactionsMap = (GOVMinimalPerfectHashFunction<MutableString>) BinIO.loadObject(artifacts.resolve("transactions.map").toFile());
		GOVMinimalPerfectHashFunction<MutableString> addressMap = (GOVMinimalPerfectHashFunction<MutableString>) BinIO.loadObject(artifacts.resolve("addresses.map").toFile());
		Object2IntFunction<? extends CharSequence> addressFunction = (a) -> (int) addressMap.getLong(a);
		LabelMapping labelMapping = (l, t) -> ((GammaCodedIntLabel) l).value = (int) transactionsMap.getLong(t);
		int numNodes = (int) addressMap.size64();

		Logger logger = LoggerFactory.getLogger(TransactionGraph.class);
		ProgressLogger pl = new ProgressLogger(logger, 10, TimeUnit.MINUTES);

		TransactionGraph graph = new TransactionGraph(Files.newInputStream(inputsFile), Files.newInputStream(outputsFile), addressFunction, null, numNodes, DEFAULT_LABEL_PROTOTYPE, labelMapping, 50, null, pl);
		System.out.println(graph);
		BVGraph.storeLabelled(graph.arcLabelledBatchGraph, graphDir.resolve("bitcoin").toString(), graphDir.resolve("bitcoin-underlying").toString());
	}
}