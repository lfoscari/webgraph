package it.unimi.dsi.webgraph.examples;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.objects.Object2IntFunction;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import it.unimi.dsi.webgraph.labelling.*;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TransactionGraphExample {
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

		FastByteArrayInputStream inp = new FastByteArrayInputStream(("t1 911\n" +
				"t1 912\n" +
				"t2 921\n" +
				"t2 922").getBytes());
		FastByteArrayInputStream oup = new FastByteArrayInputStream(("t1 811\n" +
				"t1 812\n" +
				"t2 821\n" +
				"t2 822").getBytes());

		FastByteArrayInputStream arcs = new FastByteArrayInputStream(("911 811 t1\n" +
				"911 812 t1\n" +
				"912 811 t1\n" +
				"912 812 t1\n" +
				"921 821 t2\n" +
				"921 822 t2\n" +
				"922 821 t2\n" +
				"922 822 t2\n").getBytes());


		Object2IntFunction<? extends CharSequence> addressIntMap = (n) -> (int) addressMap.getLong(n);

		Object2IntFunction<? extends CharSequence> transactionMap = (s) -> Integer.parseInt(((String) s).substring(1));

		Label prototype = new GammaCodedIntLabel("transaction-id");

		ScatteredLabelledArcsASCIIGraph.LabelMapping labelMapping = (label, transaction) ->
				((GammaCodedIntLabel) label).value = transactionMap.getInt(transaction);

		System.out.println();
		TransactionGraph g1 = new TransactionGraph(inp, oup, addressIntMap, Charset.defaultCharset(), 8, prototype, labelMapping, 10, null, null);

		System.out.println(g1);

		ScatteredLabelledArcsASCIIGraph g2 = new ScatteredLabelledArcsASCIIGraph(arcs, addressMap, Charset.defaultCharset(), 8, prototype, labelMapping, null, false, false, 10);

		System.out.println(g2);
	}
}
