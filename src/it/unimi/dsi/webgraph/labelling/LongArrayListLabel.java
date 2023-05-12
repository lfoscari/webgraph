package it.unimi.dsi.webgraph.labelling;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.IOException;

public class LongArrayListLabel extends AbstractLongListLabel {
	public LongArrayListLabel(String key, LongArrayList longs) {
		super(key, longs);
	}

	public LongArrayListLabel(final String ...arg) {
		this(arg[0], LongArrayList.of(Long.parseLong(arg[1])));
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
		int bitsWritten = outputBitStream.writeDelta(values.size());
		for (long l: values)
			bitsWritten += outputBitStream.writeLongDelta(l);
		return bitsWritten;
	}

	@Override
	public Label copy() {
		return new LongArrayListLabel(key, new LongArrayList(values));
	}

	@Override
	public int fixedWidth() {
		return -1;
	}

	/** Join the lists from this and another label.
	 * @return the current label.
	 */
	public LongArrayListLabel merge(LongArrayListLabel other) {
		values.addAll(other.values);
		return this;
	}

	/** Add an element to the list of longs.
	 * @return the current label.
	 */
	public LongArrayListLabel add(final long l) {
		this.values.add(l);
		return this;
	}

	/** Reinitialize the current label from the given longs
	 * @return the given long.
	 */
	public long init(final long l) {
		this.values.size(1);
		this.values.set(0, l);
		return l;
	}
}
