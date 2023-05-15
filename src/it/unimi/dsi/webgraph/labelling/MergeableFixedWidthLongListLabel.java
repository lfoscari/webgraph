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

import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.IOException;

/** A list of longs represented in fixed width. The provided width must
 * be smaller than 64. Each list is prefixed by its length written
 * in {@linkplain OutputBitStream#writeGamma(int) &gamma; coding}.
 * This version of the class provides the ability to merge two
 * existing instances.
 */

public class MergeableFixedWidthLongListLabel extends FixedWidthLongListLabel {
	public int size = 0;

	public MergeableFixedWidthLongListLabel(final String key, final int width) {
		super(key, width);
	}

	public MergeableFixedWidthLongListLabel(final String key, final int width, final long[] value) {
		super(key, width, value);
		size = value.length;
	}

	@Override
	public Label copy() {
		return new MergeableFixedWidthLongListLabel(key, width, value.clone());
	}

	@Override
	public int fromBitStream(final InputBitStream inputBitStream, final int sourceUnused) throws IOException {
		int readBits = super.fromBitStream(inputBitStream, sourceUnused);
		size = value.length;
		return readBits;
	}

	public MergeableFixedWidthLongListLabel merge(MergeableFixedWidthLongListLabel other) {
		value = LongArrays.grow(value, size + other.size);
		System.arraycopy(other.value, 0, value, size, other.size);
		size += other.size;
		return this;
	}
}
