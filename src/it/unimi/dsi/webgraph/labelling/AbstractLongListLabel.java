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

import it.unimi.dsi.fastutil.longs.LongList;

import java.util.NoSuchElementException;

/** An abstract (single-attribute) longs label.
 *
 * <p>This class provides basic methods for a label holding multiple longs.
 * Concrete implementations may impose further requirements on the longs.
 *
 * <p>Implementing subclasses must provide constructors, {@link Label#copy()},
 * {@link Label#fromBitStream(it.unimi.dsi.io.InputBitStream, int)}, {@link Label#toBitStream(it.unimi.dsi.io.OutputBitStream, int)}
 * and possibly override {@link #toString()}.
 */

public abstract class AbstractLongListLabel extends AbstractLabel implements Label {
	/** The key of the attribute represented by this label. */
	protected final String key;
	/** The value of the attribute represented by this label. */
	public LongList values;

	/** Creates a new longs label from a list of longs.
	 *
	 * @param key the (only) key of this label.
	 * @param values the values of this label.
	 */
	public AbstractLongListLabel(final String key, final LongList values) {
		this.key = key;
		this.values = values;
	}

	/** Creates a new longs label from a long array.
	 *
	 * @param key the (only) key of this label.
	 * @param values the values of this label.
	 */
	public AbstractLongListLabel(final String key, final long ...values) {
		this(key, LongList.of(values));
	}

	/** Creates a new longs label without any values.
	 *
	 * @param key the (only) key of this label.
	 */
	public AbstractLongListLabel(final String key) {
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
		return new Class<?>[] { values.getClass() };
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

	@Override
	public boolean equals(final Object x) {
		if (x instanceof AbstractLongListLabel) return (values.equals(((AbstractLongListLabel)x).values));
		else return false;
	}

	@Override
	public int hashCode() {
		return values.hashCode();
	}
}
