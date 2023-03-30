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

import com.martiansoftware.jsap.*;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOV3Function;
import it.unimi.dsi.webgraph.*;
import it.unimi.dsi.webgraph.labelling.ScatteredLabelledArcsASCIIGraph.LabelMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static it.unimi.dsi.fastutil.io.FastBufferedInputStream.*;
import static it.unimi.dsi.webgraph.Transform.processTransposeBatch;
import static it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph.UNDERLYINGGRAPH_SUFFIX;
import static it.unimi.dsi.webgraph.labelling.ScatteredLabelledArcsASCIIGraph.getLong;

// TODO: Description and methods spec

public class TransactionInputsOutputsASCIIGraph extends ImmutableSequentialGraph {
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
	public static final LabelMapping DEFAULT_LABEL_MAPPING = ScatteredLabelledArcsASCIIGraph.DEFAULT_LABEL_MAPPING;
	/**
	 * The default logger.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(TransactionInputsOutputsASCIIGraph.class);
	/**
	 * Toggle the debug mode.
	 */
	private final static boolean DEBUG = false;
	/**
	 * The extension of the identifier file (a binary list of longs).
	 */
	private static final String IDS_EXTENSION = ".ids";
	/**
	 * The labelled batch graph used to return node iterators.
	 */
	public final Transform.ArcLabelledBatchGraph arcLabelledBatchGraph;
	/**
	 * The list of addresses in order of appearance.
	 */
	public long[] addresses;

	public TransactionInputsOutputsASCIIGraph(final InputStream inputsIs, final InputStream outputsIs) throws IOException {
		this(inputsIs, outputsIs, null, -1, DEFAULT_LABEL_PROTOTYPE, DEFAULT_LABEL_MAPPING, null);
	}

	public TransactionInputsOutputsASCIIGraph(final InputStream inputsIs, final InputStream outputsIs, final Object2LongFunction<byte[]> addressMap, final int numNodes) throws IOException {
		this(inputsIs, outputsIs, addressMap, numNodes, DEFAULT_LABEL_PROTOTYPE, DEFAULT_LABEL_MAPPING, null);
	}

	public TransactionInputsOutputsASCIIGraph(final InputStream inputsIs, final InputStream outputsIs, final Object2LongFunction<byte[]> addressMap, final int numNodes, final Label labelPrototype, final LabelMapping labelMapping, final LabelMergeStrategy labelMergeStrategy) throws IOException {
		this(inputsIs, outputsIs, addressMap, numNodes, labelPrototype, labelMapping, labelMergeStrategy, DEFAULT_BATCH_SIZE, null, null, null);
	}

	public TransactionInputsOutputsASCIIGraph(
			final InputStream inputsIs,
			final InputStream outputsIs,
			final Object2LongFunction<byte[]> addressMap,
			int numNodes,
			final Label labelPrototype,
			final LabelMapping labelMapping,
			final LabelMergeStrategy labelMergeStrategy,
			final int batchSize,
			final Statistics statistics,
			final File tempDir,
			final ProgressLogger pl) throws IOException {

		if (addressMap != null && numNodes < 0) {
			throw new IllegalArgumentException("Negative number of nodes");
		}

		ScatteredArcsASCIIGraph.Id2NodeMap map = new ScatteredArcsASCIIGraph.Id2NodeMap();

		final ReadTransactions inputs = new ReadTransactions(new FastBufferedInputStream(inputsIs), addressMap, numNodes, map);
		final ReadTransactions outputs = new ReadTransactions(new FastBufferedInputStream(outputsIs), addressMap, numNodes, map);

		int j = 0;
		long pairs = 0;

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

		for (;;) {
			if (DEBUG) System.out.println("input");
			final IntArrayList inputAddresses = inputs.nextAddresses();

			if (inputAddresses.size() == 0) {
				if (DEBUG) System.out.println("no more transactions inputs, terminating...");
				break; // outputs EOF
			}

			if (DEBUG) System.out.println("output");
			final IntArrayList outputAddresses = outputs.nextAddresses(inputs.currentLine, inputs.transactionStart, inputs.transactionEnd);

			if (DEBUG) System.out.println();

			if (outputAddresses.size() == 0) {
				LOGGER.warn("Inconsistency: Couldn't find matching transaction!\n"
						+ "\tinput at line " + inputs.lineNumber() + ":\t" + inputs.line() + "\n"
						+ "\toutput at line " + outputs.lineNumber() + ":\t" + outputs.line() + "\n");
				continue;
			}

			// Set the label as the transaction
			byte[] transaction = inputs.transactionBytes();
			labelMapping.apply(prototype, transaction);

			if (statistics != null) {
				statistics.update(transaction, inputAddresses, outputAddresses);
			}

			for (int s : inputAddresses) {
				for (int t : outputAddresses) {
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
						pairs += processTransposeBatch(batchSize, source, target, start, new InputBitStream(fbos.array), tempDir, batches, labelBatches, prototype, labelMergeStrategy);
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
			pairs += processTransposeBatch(j, source, target, start, new InputBitStream(fbos.array), tempDir, batches, labelBatches, prototype, labelMergeStrategy);
		}

		if (pl != null) {
			pl.done();
			logBatches(batches, pairs, pl);
		}

		if (addressMap == null) {
			numNodes = (int) map.size();
			addresses = map.getIds(tempDir);
		}

		source = null;
		target = null;
		start = null;

		this.arcLabelledBatchGraph = new Transform.ArcLabelledBatchGraph(numNodes, pairs, batches, labelBatches, prototype, labelMergeStrategy);
	}

	protected static void logBatches(final ObjectArrayList<File> batches, final long pairs, final ProgressLogger pl) {
		long length = 0;
		for (final File f : batches) length += f.length();
		pl.logger().info("Created " + batches.size() + " batches using " + Util.format((double) Byte.SIZE * length / pairs) + " bits/arc.");
	}

	public static int batchSize(long transactionAmount) {
		return (int) ((1L << 31) - 1024) / (64 - Long.numberOfLeadingZeros(transactionAmount - 1));
	}

	public static void main(final String[] args) throws IllegalArgumentException, SecurityException, IOException, JSAPException, ClassNotFoundException {
		final SimpleJSAP jsap = new SimpleJSAP(ScatteredLabelledArcsASCIIGraph.class.getName(),
				"Converts the list of inputs and outputs from a cryptocurrency into a labelled BVGraph. Both inputs" +
						"and outputs must be written as a list of transaction-address pairs separated by a single tab " +
						"character and sorted by transaction, each node is an address and the arcs are labelled with the " +
						"transactions. The list of addresses in order of appearance will be saved with extension " +
						"\"IDS_EXTENSION\" unless a translation function has been specified. If a map from transactions " +
						"to longs is provided it is possible to compute the maximum batch size, optimal label mapping " +
						"strategy and optimal label prototype, storing the transactions longs with only as little bits " +
						"as necessary. The inputs and outputs should be void of duplicates, but in case of duplicate arcs " +
						"the label merge strategy will be invoked, the last occurring label is kept if no merge strategy " +
						"is provided. It is possible to provide a directory to store the statistics computed during " +
						"computation. The underlying representation of the labels will be saved as the given basename " +
						"with the \"UNDERLYINGGRAPH_SUFFIX\" suffix.",
				new Parameter[]{
						new FlaggedOption("logInterval", JSAP.LONG_PARSER, Long.toString(ProgressLogger.DEFAULT_LOG_INTERVAL), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds."),
						new FlaggedOption("batchSize", JSAP.INTSIZE_PARSER, Integer.toString(DEFAULT_BATCH_SIZE), JSAP.NOT_REQUIRED, 's', "batch-size", "The maximum size of a batch, in arcs."),
						new FlaggedOption("tempDir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'T', "temp-dir", "A directory for all temporary batch files."),
						new FlaggedOption("addressMap", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'a', "addressMap", "A serialised function from strings to longs that will be used to translate addresses to node numbers."),
						new FlaggedOption("transactionMap", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 't', "transactionMap", "A serialised function from strings to longs that will be used to translate addresses to node numbers."),
						new FlaggedOption("n", JSAP.INTSIZE_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'n', "n", "The number of nodes of the graph (only if you specified a function that does not return the size of the key set, or if you want to override that size)."),
						new FlaggedOption("comp", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'c', "comp", "A compression flag (may be specified several times).").setAllowMultipleDeclarations(true),
						new FlaggedOption("windowSize", JSAP.INTEGER_PARSER, String.valueOf(BVGraph.DEFAULT_WINDOW_SIZE), JSAP.NOT_REQUIRED, 'w', "window-size", "Reference window size (0 to disable)."),
						new FlaggedOption("maxRefCount", JSAP.INTEGER_PARSER, String.valueOf(BVGraph.DEFAULT_MAX_REF_COUNT), JSAP.NOT_REQUIRED, 'm', "max-ref-count", "Maximum number of backward references (-1 for ∞)."),
						new FlaggedOption("minIntervalLength", JSAP.INTEGER_PARSER, String.valueOf(BVGraph.DEFAULT_MIN_INTERVAL_LENGTH), JSAP.NOT_REQUIRED, 'i', "min-interval-length", "Minimum length of an interval (0 to disable)."),
						new FlaggedOption("zetaK", JSAP.INTEGER_PARSER, String.valueOf(BVGraph.DEFAULT_ZETA_K), JSAP.NOT_REQUIRED, 'k', "zeta-k", "The k parameter for zeta-k codes."),
						new FlaggedOption("labelPrototype", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "label-prototype", "The prototype of the labels."),
						new FlaggedOption("labelMapping", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'M', "label-mapping", "A serialised function from strings to the given label prototype that will be used to translate label strings to label object."),
						new FlaggedOption("labelMergeStrategy", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'e', "label-merge-strategy", "A serialized LabelMergeStrategy object defining how to treat duplicate arcs with the same label."),
						new FlaggedOption("statistics", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'S', "statistics", "A directory for the statistics gathered during computation"),
						new UnflaggedOption("inputs", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The inputs of the transactions in the blockchain, sorted by transaction."),
						new UnflaggedOption("outputs", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The outputs of the transactions in the blockchain, sorted by transaction."),
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the output graph"),
				}
		);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) {
			System.exit(1);
		}

		String basename = jsapResult.getString("basename");

		File inputs = new File(jsapResult.getString("inputs"));
		if (!inputs.exists()) throw new JSAPException(inputs + " file does not exist");

		File outputs = new File(jsapResult.getString("outputs"));
		if (!outputs.exists()) throw new JSAPException(outputs + " file does not exist");

		int flags = 0;
		for(final String compressionFlag: jsapResult.getStringArray("comp"))
			try {
				flags |= BVGraph.class.getField(compressionFlag).getInt(BVGraph.class);
			} catch (final Exception notFound) {
				throw new JSAPException("Compression method " + compressionFlag + " unknown.");
			}

		final int windowSize = jsapResult.getInt("windowSize");
		final int zetaK = jsapResult.getInt("zetaK");
		int maxRefCount = jsapResult.getInt("maxRefCount");
		if (maxRefCount == -1) maxRefCount = Integer.MAX_VALUE;
		final int minIntervalLength = jsapResult.getInt("minIntervalLength");

		Object2LongFunction<byte[]> addressMap = null;
		int n = -1;

		if (jsapResult.userSpecified("addressMap")) {
			addressMap = (Object2LongFunction<byte[]>) BinIO.loadObject(jsapResult.getString("addressMap"));
			if (addressMap.size() == -1) {
				if (!jsapResult.userSpecified("n")) {
					throw new IllegalArgumentException("You must specify a graph size if you specify a translation function that does not return the size of the key set.");
				}
				n = jsapResult.getInt("n");
			} else {
				n = addressMap.size();
			}
		}

		Object2LongFunction<byte[]> transactionMap;
		if (jsapResult.userSpecified("transactionMap")) {
			transactionMap = (Object2LongFunction<byte[]>) BinIO.loadObject(jsapResult.getString("transactionMap"));
		} else {
			transactionMap = null;
		}

		if (jsapResult.userSpecified("labelPrototype") != jsapResult.userSpecified("labelMapping")) {
			throw new IllegalArgumentException("You must specify either both a label prototype and a label mapping or none.");
		}

		Label labelPrototype = DEFAULT_LABEL_PROTOTYPE;
		if (jsapResult.userSpecified("labelPrototype")) {
			labelPrototype = (Label) BinIO.loadObject(jsapResult.getString("labelPrototype"));
		} else if (transactionMap != null && transactionMap.size() != -1) {
			int maxBitsForTransactions = 64 - Long.numberOfLeadingZeros(transactionMap.size() - 1);
			labelPrototype = new FixedWidthLongLabel("transaction-id", maxBitsForTransactions);
		}

		LabelMapping labelMapping = DEFAULT_LABEL_MAPPING;
		if (jsapResult.userSpecified("labelMapping")) {
			labelMapping = (LabelMapping) BinIO.loadObject(jsapResult.getString("labelMapping"));
		} else if (transactionMap != null) {
			labelMapping = (prototype, transaction) -> ((FixedWidthLongLabel) prototype).value = transactionMap.getLong(transaction);
		}

		LabelMergeStrategy labelMergeStrategy = null;
		if (jsapResult.userSpecified("labelMergeStrategy")) {
			labelMergeStrategy = (LabelMergeStrategy) BinIO.loadObject(jsapResult.getString("labelMergeStrategy"));
		}

		File tempDir = null;
		if (jsapResult.userSpecified("tempDir")) {
			tempDir = new File(jsapResult.getString("tempDir"));
		}

		int batchSize = -1;
		if (jsapResult.userSpecified("batchSize")) {
			batchSize = jsapResult.getInt("batchSize");
		} else if (transactionMap != null && transactionMap.size() != -1) {
			batchSize = batchSize(transactionMap.size());
		}

		Statistics statistics = null;
		if (jsapResult.userSpecified("statistics")) {
			File statDir = new File(jsapResult.getString("statistics"));
			statDir.mkdir();
			statistics = new Statistics(statDir.toPath(), transactionMap);
		}

		final ProgressLogger pl = new ProgressLogger(LOGGER, jsapResult.getLong("logInterval"), TimeUnit.MILLISECONDS);
		pl.displayFreeMemory = true;

		final TransactionInputsOutputsASCIIGraph graph = new TransactionInputsOutputsASCIIGraph(Files.newInputStream(inputs.toPath()), Files.newInputStream(outputs.toPath()), addressMap, n, labelPrototype, labelMapping, labelMergeStrategy, batchSize, statistics, tempDir, pl);
		BVGraph.storeLabelled(graph.arcLabelledBatchGraph, basename, basename + UNDERLYINGGRAPH_SUFFIX, windowSize, maxRefCount, minIntervalLength, zetaK, flags, pl);

		if (addressMap == null) {
			BinIO.storeLongs(graph.addresses, basename + IDS_EXTENSION);
		}
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
	public TransactionInputsOutputsASCIIGraph copy() {
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

	private static class Statistics implements Closeable {
		private final Object2LongFunction<byte[]> transactionMap;

		private final FastBufferedOutputStream amountInputsOutputs;
		private final FastBufferedOutputStream duplicateInputsOutputs;

		public Statistics(Path statisticsDirectory, Object2LongFunction<byte[]> transactionMap) throws IOException {
			this.transactionMap = transactionMap;
			amountInputsOutputs = new FastBufferedOutputStream(Files.newOutputStream(statisticsDirectory.resolve("amounts")));
			duplicateInputsOutputs = new FastBufferedOutputStream(Files.newOutputStream(statisticsDirectory.resolve("duplicates")));
		}

		public void update(byte[] transaction, IntArrayList inputAddresses, IntArrayList outputAddresses) throws IOException {
			long uniqueInputs = uniqueAddressesAmount(inputAddresses),
				uniqueOutputs = uniqueAddressesAmount(outputAddresses);

			MutableString mb = new MutableString();
			mb.append(transactionMap != null ? transactionMap.getLong(transaction) : transaction);
			mb.append(uniqueInputs);
			mb.append(uniqueOutputs);
			mb.append("\n");
			mb.writeSelfDelimUTF8(amountInputsOutputs);

			mb.length(0);
			mb.append(transactionMap != null ? transactionMap.getLong(transaction) : transaction);
			mb.append(inputAddresses.size() - uniqueInputs);
			mb.append(outputAddresses.size() - uniqueOutputs);
			mb.append("\n");
			mb.writeSelfDelimUTF8(duplicateInputsOutputs);
		}

		private static long uniqueAddressesAmount(final IntArrayList addresses) {
			if (addresses.size() == 0) return 0;
			return new IntOpenHashSet(addresses).size();
		}

		@Override
		public void close() throws IOException {
			this.amountInputsOutputs.close();
			this.duplicateInputsOutputs.close();
		}
	}

	public static class ReadTransactions {
		private final FastBufferedInputStream stream;
		private final Object2LongFunction<byte[]> addressMap;
		private final int numNodes;

		private final IntArrayList addresses = new IntArrayList(512);
		private final ScatteredArcsASCIIGraph.Id2NodeMap map;
		private int lineLength;
		private int offset;
		private int line = 1;

		// TODO: make private
		public int tmpTransactionStart = -1, tmpTransactionEnd = -1;
		public byte[] tmpTransaction = null;

		public int transactionStart = -1, transactionEnd = -1;
		public byte[] previousLine = new byte[1024];
		public byte[] currentLine = new byte[1024];

		public ReadTransactions(FastBufferedInputStream stream, final Object2LongFunction<byte[]> addressMap) throws IOException {
			this(stream, addressMap, Integer.MAX_VALUE, null);
		}

		public ReadTransactions(FastBufferedInputStream stream, final Object2LongFunction<byte[]> addressMap, final int numNodes, final ScatteredArcsASCIIGraph.Id2NodeMap map) throws IOException {
			this.stream = stream;
			this.addressMap = addressMap;
			this.numNodes = numNodes;
			this.map = map;

			do {
				int start = 0, len;
				while ((len = stream.readLine(previousLine, start, previousLine.length - start, ALL_TERMINATORS)) == currentLine.length - start) {
					start += len;
					previousLine = ByteArrays.grow(previousLine, previousLine.length + 1);
				}

				if (len == -1) {
					throw new RuntimeException(stream + " is empty!");
				}
				lineLength = start + len;
			} while (skipWhitespace(previousLine) == -1);
		}

		public IntArrayList nextAddresses() throws IOException {
			return nextAddresses(null, -1, -1);
		}

		public IntArrayList nextAddresses(byte[] transaction, int from, int to) throws IOException {
			addresses.clear();

			tmpTransaction = transaction;
			tmpTransactionStart = from;
			tmpTransactionEnd = to;

			if (lineLength == -1) {
				return addresses;
			}

			offset = 0;
			skipWhitespace(previousLine);

			// Scan transaction
			int start = offset;
			while (offset < lineLength && (previousLine[offset] < 0 || previousLine[offset] > ' ')) offset++;

			// Save the boundaries of the transaction in previousLine
			transactionStart = start;
			transactionEnd = offset;

			int cmp = transaction != null ? Arrays.compare(previousLine, transactionStart, transactionEnd, transaction, from, to) : -1;
			if (transaction == null || cmp == 0) {
				if (DEBUG) {
					if (transactionStart != -1) {
						System.out.print("t: " + new String(previousLine, transactionStart, transactionEnd - transactionStart) + "\t");
					} else if (transaction != null) {
						System.out.print("t: " + new String(transaction) + "\t");
					} else {
						System.out.print("t: " + transaction(Charset.defaultCharset()) + "\t");
					}
				}

				// Add previousLine address to address list
				skipWhitespace(previousLine);
				addAddressFromLine(previousLine);
			}

			if (cmp > 0) {
				return addresses;
			}

			for (;;) {
				// Now keep reading lines into currentLine until the transaction matches the one in previousLine
				offset = 0;

				do {
					int lineStart = 0, len;
					while ((len = stream.readLine(currentLine, lineStart, currentLine.length - lineStart, ALL_TERMINATORS)) == currentLine.length - lineStart) {
						lineStart += len;
						currentLine = ByteArrays.grow(currentLine, currentLine.length + 1);
					}

					if (len == -1) {
						lineLength = -1; // EOF
						break;
					}
					lineLength = lineStart + len;
					line++;
				} while (skipWhitespace(currentLine) == -1);

				if (lineLength == -1) { // EOF
					currentLine = previousLine;
					return addresses;
				}

				// Scan transaction
				start = offset;
				while (offset < lineLength && (currentLine[offset] < 0 || currentLine[offset] > ' ')) offset++;

				if (DEBUG) System.out.print("t: " + new String(currentLine, start, offset - start) + "\t");

				// Check if transactions match
				cmp = transaction == null ?
					Arrays.compare(currentLine, start, offset, previousLine, transactionStart, transactionEnd) :
					Arrays.compare(currentLine, start, offset, transaction, from, to);

				if (cmp < 0) {
					if (DEBUG) System.out.print("skipped ");
					continue;
				} else if (cmp > 0) {
					break;
				}

				skipWhitespace(currentLine);
				addAddressFromLine(currentLine);
			}

			if (DEBUG) System.out.println();

			byte[] t = currentLine;
			currentLine = previousLine;
			previousLine = t;

			return addresses;
		}

		private void addAddressFromLine(final byte[] array) {
			final int start = offset;
			while (offset < lineLength && (array[offset] < 0 || array[offset] > ' ')) offset++;

			int addressId;

			if (addressMap == null) {
				final long id = getLong(array, start, offset - start);
				addressId = map.getNode(id);
			} else {
				final byte[] address = Arrays.copyOfRange(array, start, offset);
				addressId = (int) addressMap.getLong(address);

				if (addressId < 0 || addressId >= numNodes) {
					throw new IllegalArgumentException("Address node number out of range for node " + addressId + ": " + new String(array, start, offset - start));
				}
			}

			addresses.add(addressId);
			if (DEBUG) System.out.println("a: " + new String(array, start, offset - start) + " [" + addressId + "]");
		}

		private int skipWhitespace(final byte[] array) {
			while (offset < lineLength && array[offset] >= 0 && array[offset] <= ' ') offset++;
			if (offset == lineLength || array[0] == '#') return -1;
			return offset;
		}

		public byte[] transactionBytes() {
			if (transactionStart == -1) throw new IllegalStateException("You must first read addresses");
			return tmpTransaction == null ?
					Arrays.copyOfRange(currentLine, transactionStart, transactionEnd) :
					Arrays.copyOfRange(tmpTransaction, tmpTransactionStart, tmpTransactionEnd);
		}

		public String transaction(Charset charset) {
			if (transactionStart == -1) throw new IllegalStateException("You must first read addresses");
			return new String(transactionBytes(), charset);
		}

		public String line() {
			if (lineLength != -1) {
				return new String(previousLine, 0, lineLength);
			}

			int offset = 0;
			while (offset < currentLine.length && currentLine[offset] >= 0 && currentLine[offset] <= ' ') offset++;
			final int start = offset;
			while (offset < currentLine.length && (currentLine[offset] < 0 || currentLine[offset] > ' ')) offset++;
			while (offset < currentLine.length && currentLine[offset] >= 0 && currentLine[offset] <= ' ') offset++;
			while (offset < currentLine.length && (currentLine[offset] < 0 || currentLine[offset] > ' ')) offset++;

			return new String(currentLine, start, offset - start);
		}

		public int lineNumber() {
			return line;
		}
	}
}