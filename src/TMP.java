import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.objects.Object2IntFunction;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.LineIterator;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import it.unimi.dsi.webgraph.labelling.*;
import org.apache.commons.math3.analysis.function.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class TMP {
	public static void main(String[] args) throws IOException {
		GOVMinimalPerfectHashFunction.Builder<MutableString> b = new GOVMinimalPerfectHashFunction.Builder<>();
		List<MutableString> tarcs = new ArrayList<>();
		for (String s1 : Arrays.asList("911", "912", "921", "922", "811", "812", "821", "822")) {
			MutableString mutableString = new MutableString(s1);
			tarcs.add(mutableString);
		}
		b.keys(tarcs);
		b.transform(TransformationStrategies.iso());
		GOVMinimalPerfectHashFunction<MutableString> addressMap = b.build();

		String i = "911 t1 \n 912 t1 \n 921 t2 \n 922 t2";
		String o = "811 t1 \n 812 t1 \n 821 t2 \n 822 t2";

		FastByteArrayInputStream arcs = new FastByteArrayInputStream(("911 811 t1\n" +
																	  "911 812 t1\n" +
																	  "912 811 t1\n" +
																	  "912 812 t1\n" +
																	  "921 821 t2\n" +
																	  "921 822 t2\n" +
																	  "922 821 t2\n" +
																	  "922 822 t2\n").getBytes());

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

		Object2IntFunction<? extends CharSequence> addressIntMap = (n) -> (int) addressMap.getLong(n);

		Object2IntFunction<? extends CharSequence> transactionMap = (s) -> Integer.parseInt(((String) s).substring(1));

		Label prototype = new GammaCodedIntLabel("transaction-id");

		ScatteredLabelledArcsASCIIGraph.LabelMapping labelMapping = (label, transaction) ->
				((GammaCodedIntLabel) label).value = transactionMap.getInt(transaction);

		System.out.println("TRANSACTION");
		TransactionGraph g = new TransactionGraph(inputs, outputs, addressIntMap, 8, prototype, labelMapping, 10, null, null);

		System.out.println("SCATTERED");
		ScatteredLabelledArcsASCIIGraph graph = new ScatteredLabelledArcsASCIIGraph(arcs, addressMap, Charset.defaultCharset(), 8, prototype, labelMapping, null, false, false, 10);

		in.delete();
		out.delete();
	}
}
