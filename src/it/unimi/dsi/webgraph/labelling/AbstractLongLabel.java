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

/** An abstract (single-attribute) long label.
 *
 * <p>This class provides basic methods for a label holding a long.
 * Concrete implementations may impose further requirements on the long.
 *
 * <p>Implementing subclasses must provide constructors, {@link Label#copy()},
 * {@link Label#fromBitStream(it.unimi.dsi.io.InputBitStream, int)}, {@link Label#toBitStream(it.unimi.dsi.io.OutputBitStream, int)}
 * and possibly override {@link #toString()}.
 */

public abstract class AbstractLongLabel extends AbstractLabel implements Label {
	/** The key of the attribute represented by this label. */
	protected final String key;
	/** The value of the attribute represented by this label. */
	public long value;

	/** Creates an int label with given key and value.
	 *
	 * @param key the (only) key of this label.
	 * @param value the value of this label.
	 */
	public AbstractLongLabel(final String key, final long value) {
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
		return new Class<?>[] { long.class };
	}

	@Override
	public Object get(final String key) {
		return getLong(key);
	}

	@Override
	public long getLong(final String key) {
		if (this.key.equals(key)) return value;
		throw new IllegalArgumentException("Unknown key " + key);
	}

	@Override
	public int getInt(final String key) {
		return (int) getLong(key);
	}

	@Override
	public float getFloat(final String key) {
		return getInt(key);
	}

	@Override
	public double getDouble(final String key) {
		return getInt(key);
	}

	@Override
	public Object get() {
		return getLong();
	}

	@Override
	public int getInt() {
		return (int) value;
	}

	@Override
	public long getLong() {
		return value;
	}

	@Override
	public float getFloat() {
		return value;
	}

	@Override
	public double getDouble() {
		return value;
	}

	@Override
	public String toString() {
		return key + ":" + value;
	}

	@Override
	public boolean equals(final Object x) {
		if (x instanceof AbstractLongLabel) return (value == ((AbstractLongLabel)x).value);
		else return false;
	}

	@Override
	public int hashCode() {
		return Long.hashCode(value);
	}
}
