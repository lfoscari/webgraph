import it.unimi.dsi.fastutil.objects.Object2IntFunction;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.LineIterator;
import it.unimi.dsi.webgraph.labelling.*;

import java.io.*;
import java.util.Arrays;

public class TMP {
	public static void main(String[] args) throws IOException {
		String i = "i11 t1 \n i12 t1 \n i21 t2 \n i22 t2";
		String o = "o11 t1 \n o12 t1 \n i21 t2 \n i22 t2";

		File in = File.createTempFile("arcs", ".input");
		try (FileOutputStream fos = new FileOutputStream(in)) {
			fos.write(i.getBytes());
		}
		LineIterator inputs = new LineIterator(new FastBufferedReader(new FileReader(in)));

		File out = File.createTempFile("arcs", ".output");
		try (FileOutputStream fos = new FileOutputStream(out)) {
			fos.write(o.getBytes());
		}
		LineIterator outputs = new LineIterator(new FastBufferedReader(new FileReader(out)));

		Object2IntFunction<? extends CharSequence> addressMap = (s) -> {
			CharSequence l = (CharSequence) s;
			return Integer.parseInt(String.valueOf(l.subSequence(1, l.length())));
		};

		Object2IntFunction<? extends CharSequence> transactionMap = (s) -> {
			CharSequence l = (CharSequence) s;
			return Integer.parseInt(String.valueOf(l.subSequence(1, l.length())));
		};

		Label prototype = new GammaCodedIntLabel("transaction-id");

		ScatteredLabelledArcsASCIIGraph.LabelMapping labelMapping = (label, transaction) ->
				((GammaCodedIntLabel) label).value = transactionMap.getInt(transaction);

		TransactionGraph g = new TransactionGraph(
				inputs, outputs, addressMap, 8,
				prototype, labelMapping,
				5, null, null
		);

		ArcLabelledNodeIterator iterator = g.nodeIterator();
		int j = 11;
		while (--j != 0) iterator.nextInt();

		System.out.println(Arrays.toString(iterator.successorArray()));
		System.out.println(Arrays.toString(iterator.labelArray()));

		in.delete();
		out.delete();
	}
}
