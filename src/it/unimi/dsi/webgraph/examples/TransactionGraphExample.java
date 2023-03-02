package it.unimi.dsi.webgraph.examples;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.objects.Object2IntFunction;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import it.unimi.dsi.webgraph.labelling.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TransactionGraphExample {

	public static final List<String> KEYS = Arrays.asList("911", "912", "921", "922", "811", "812", "821", "822");

	public static void main(String[] args) throws IOException {
		GOVMinimalPerfectHashFunction.Builder<MutableString> b = new GOVMinimalPerfectHashFunction.Builder<>();
		List<MutableString> tarcs = new ArrayList<>();
		for (String s1 : KEYS) {
			MutableString mutableString = new MutableString(s1);
			tarcs.add(mutableString);
		}
		b.keys(tarcs);
		b.transform(TransformationStrategies.iso());
		GOVMinimalPerfectHashFunction<MutableString> addressMap = b.build();

		for (String key: KEYS) {
			System.out.println(key + " => " + addressMap.getLong(key));
		}

		FastByteArrayInputStream inputs = new FastByteArrayInputStream((
				"t1 911\n" +
				"t1 912\n" +
				"t2 921\n" +
				"t2 922\n" +
				"t4 922").getBytes());
		FastByteArrayInputStream outputs = new FastByteArrayInputStream((
				"t1 811\n" +
				"t1 812\n" +
				"t2 821\n" +
				"t2 822\n" +
				"t8 822").getBytes());

		FastByteArrayInputStream arcs = new FastByteArrayInputStream((
				"911 811 t1\n" +
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
		ScatteredLabelledArcsASCIIGraph.LabelMapping labelMapping = (label, transaction) -> ((GammaCodedIntLabel) label).value = transactionMap.getInt(transaction);
		Logger l = LoggerFactory.getLogger(TransactionGraph.class);
		ProgressLogger pl = new ProgressLogger(l, "transactions");

		System.out.println(new TransactionGraph(inputs, outputs, addressIntMap, Charset.defaultCharset(), (int) addressMap.size64(), prototype, labelMapping, 1, null, pl));
		System.out.println(new ScatteredLabelledArcsASCIIGraph(arcs, addressMap, Charset.defaultCharset(), (int) addressMap.size64(), prototype, labelMapping, null, false, false, 1));
	}
}
