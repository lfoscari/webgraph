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

import java.util.Arrays;
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
	public long[] value;

	/** Creates a new longs label from a list of longs.
	 *
	 * @param key the (only) key of this label.
	 * @param value the values of this label.
	 */
	public AbstractLongListLabel(final String key, final long[] value) {
		this.key = key;
		this.value = value;
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
		return new Class<?>[] { value.getClass() };
	}

	@Override
	public Object get(final String key) throws NoSuchElementException {
		if (this.key.equals(key)) return value;
		throw new IllegalArgumentException("Unknown key " + key);
	}

	@Override
	public Object get() throws NoSuchElementException {
		return value;
	}

	@Override
	public String toString() {
		return key + ":" + Arrays.toString(value);
	}

	@Override
	public boolean equals(final Object x) {
		if (x instanceof AbstractLongListLabel) return Arrays.equals(value, ((AbstractLongListLabel)x).value);
		else return false;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(value);
	}

	@Override
	public String toSpec() {
		return this.getClass().getName() + "(" + key + ")";
	}

	/** Returns the number of longs stored in this label.
	 * @return the length of this label.
	 */
	public int length() {
		return value.length;
	}

}
