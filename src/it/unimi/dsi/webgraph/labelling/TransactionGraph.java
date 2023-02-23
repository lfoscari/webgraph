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
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.objects.Object2IntFunction;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.LineIterator;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableSequentialGraph;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.labelling.ScatteredLabelledArcsASCIIGraph.LabelMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static it.unimi.dsi.webgraph.Transform.processTransposeBatch;

public class TransactionGraph extends ImmutableSequentialGraph {
	private static final Logger LOGGER = LoggerFactory.getLogger(TransactionGraph.class);
	private final static boolean DEBUG = false;

	/**
	 * The default batch size.
	 */
	public static final int DEFAULT_BATCH_SIZE = 1000000;
	/**
	 * The default label prototype.
	 */
	public static final Label DEFAULT_LABEL_PROTOTYPE = new GammaCodedIntLabel("FOO");
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
	private static final char SEPARATOR = ' ';
	/**
	 * The labelled batch graph used to return node iterators.
	 */
	private final Transform.ArcLabelledBatchGraph arcLabelledBatchGraph;
	/**
	 * The list of identifiers in order of appearance.
	 */
	public long[] ids;


	public TransactionGraph(
			final LineIterator inputs,
			final LineIterator outputs,
			final Object2IntFunction<? extends CharSequence> addressMap,
			final int numNodes,
			final Label labelPrototype,
			final LabelMapping labelMapping,
			final int batchSize,
			final File tempDir,
			final ProgressLogger pl) throws IOException {

		// Inputs and outputs are in the form: <address> <transaction> and sorted by transaction.

		// WE NEED AN ADDRESSMAP TO AVOID USING QUINTILLION BYTES ON SAVING A MAP MADE ON THE FLY.
		// TODO: add some fault tolerance to wrongly formatted inputs or outputs

		int j = 0;
		long pairs = 0; // Number of pairs

		int[] source = new int[batchSize], target = new int[batchSize];
		final long[] labelStart = new long[batchSize];

		FastByteArrayOutputStream fbos = new FastByteArrayOutputStream();
		OutputBitStream obs = new OutputBitStream(fbos);
		final ObjectArrayList<File> batches = new ObjectArrayList<>();
		final ObjectArrayList<File> labelBatches = new ObjectArrayList<>();
		final Label prototype = labelPrototype.copy();

		String transaction;
		MutableString outputLine, inputLine = null, matchingOutputLine = null;
		IntArrayList outputAddresses = IntArrayList.of(), inputAddresses = IntArrayList.of();

		while (outputs.hasNext()) {
			outputLine = outputs.next().trim();
			transaction = getTransaction(outputLine).toString();

			outputAddresses.clear();
			outputAddresses.add(getAddressId(outputLine, addressMap));

			while (outputs.hasNext() || matchingOutputLine != null) {
				if (matchingOutputLine == null) {
					matchingOutputLine = outputs.next().trim();
				}

				if (!getTransaction(matchingOutputLine).equals(transaction)) {
					break;
				}

				outputAddresses.add(getAddressId(matchingOutputLine, addressMap));
				matchingOutputLine = null;
			}

			// {outputAddresses} contains all output addresses for the {transaction} transaction
			// Find all the input addresses and add their ids to {inputAddresses}

			inputAddresses.clear();

			while (inputs.hasNext() || inputLine != null) {
				if (inputLine == null) {
					inputLine = inputs.next().trim();
				}

				if (!getTransaction(inputLine).equals(transaction)) {
					break;
				}

				inputAddresses.add(getAddressId(inputLine, addressMap));
				inputLine = null;
			}

			System.out.println("Transaction " + transaction +
					"\nInputs: " + inputAddresses +
					"\nOutputs: " + outputAddresses
			);

			// Set the label as the transaction
			labelMapping.apply(prototype, transaction);
			// TODO: write the transaction only once?

			for (int s: inputAddresses) {
				for (int t: outputAddresses) {
					source[j] = s;
					target[j] = t;
					labelStart[j] = obs.writtenBits();
					prototype.toBitStream(obs, s);
					j++;

					if (j == batchSize) {
						obs.flush();
						pairs += processTransposeBatch(batchSize, source, target, labelStart, new InputBitStream(fbos.array), tempDir, batches, labelBatches, prototype, null);
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
			pairs += processTransposeBatch(j, source, target, labelStart, new InputBitStream(fbos.array), tempDir, batches, labelBatches, prototype, null);
		}

		if (pl != null) {
			pl.done();
			logBatches(batches, pairs, pl);
		}

		source = null;
		target = null;

		System.out.println(" pairs=" + pairs);
		this.arcLabelledBatchGraph = new Transform.ArcLabelledBatchGraph(numNodes, pairs, batches, labelBatches, prototype, null);
	}

	protected static CharSequence getTransaction(final MutableString line) {
		final int middle = line.lastIndexOf(SEPARATOR);
		return line.subSequence(middle + 1, line.length());
	}

	protected static CharSequence getAddress(final MutableString line) {
		final int middle = line.indexOf(SEPARATOR);
		return line.subSequence(0, middle);
	}

	protected static int getAddressId(final MutableString line, Object2IntFunction<? extends CharSequence> addressMap) {
		return addressMap.getInt(getAddress(line));
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
}

