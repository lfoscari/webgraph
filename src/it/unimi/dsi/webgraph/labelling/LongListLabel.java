/*
 * Copyright (C) 2007-2023 Paolo Boldi and Sebastiano Vigna
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

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.IOException;
import java.util.NoSuchElementException;

/** A list of longs label.
 *
 * <p>This class provides basic methods for a label holding a variable amount of longs.
 */

public class LongListLabel extends AbstractLabel implements Label {
	/** The key of the attribute represented by this label. */
	protected final String key;
	/** The value of the attribute represented by this label. */
	public LongList values;

	/** Creates a new long list label from a list of longs.
	 *
	 * @param key the (only) key of this label.
	 * @param values the values of this label.
	 */
	public LongListLabel(final String key, final LongList values) {
		this.key = key;
		this.values = values;
	}

	/** Creates a new long list label from a long array.
	 *
	 * @param key the (only) key of this label.
	 * @param values the values of this label.
	 */
	public LongListLabel(final String key, final long ...values) {
		this(key, LongList.of(values));
	}

	/** Creates a new long list label without any values.
	 *
	 * @param key the (only) key of this label.
	 */
	public LongListLabel(final String key) {
		this(key, new long[] {});
	}

	@Override
	public String wellKnownAttributeKey() {
		return key;
	}

	@Override
	public String[] attributeKeys() {
		return new String[] { key };
	}

	@Override
	public Class<?>[] attributeTypes() {
		return new Class<?>[] { LongList.class };
	}

	@Override
	public Object get(final String key) throws NoSuchElementException {
		return getLongs(key);
	}

	public LongList getLongs(final String key) {
		if (this.key.equals(key)) return values;
		throw new IllegalArgumentException("Unknown key " + key);
	}

	@Override
	public Object get() throws NoSuchElementException {
		return getLongs();
	}

	public LongList getLongs() {
		return values;
	}

	@Override
	public Label copy() {
		return new LongListLabel(key, values);
	}

	@Override
	public int fromBitStream(final InputBitStream inputBitStream, final int sourceUnused) throws IOException {
		long start = inputBitStream.readBits();
		int length = inputBitStream.readDelta();

		values.size(length);
		for (int i = 0; i < length; i++)
			values.set(i, inputBitStream.readLongDelta());

		return (int) (inputBitStream.readBits() - start);
	}

	@Override
	public int toBitStream(final OutputBitStream outputBitStream, final int sourceUnused) throws IOException {
		int bitsWritten = outputBitStream.writeDelta(length());
		for (long l: values)
			bitsWritten += outputBitStream.writeLongDelta(l);
		return bitsWritten;

	}

	@Override
	public int fixedWidth() {
		return -1;
	}

	/** Returns the amount of longs stored in this label.
	 * @return the length of this label.
	 */
	public int length() {
		return values.size();
	}

	@Override
	public String toString() {
		return key + ":" + values;
	}

	@Override
	public String toSpec() {
		return this.getClass().getName() + "(" + key + ")";
	}
}
