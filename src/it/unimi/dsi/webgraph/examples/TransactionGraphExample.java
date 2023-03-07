package it.unimi.dsi.webgraph.examples;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.objects.Object2IntFunction;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOV3Function;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.labelling.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TransactionGraphExample {

	public static final Logger l = LoggerFactory.getLogger(TransactionGraph.class);
	public static final ProgressLogger pl = new ProgressLogger(l, "transactions");
	// public static final List<String> KEYS = Arrays.asList("911", "912", "921", "922", "811", "812", "821", "822");

	public static void main(String[] args) throws IOException {

		Object2IntFunction<byte[]> addressMap = (bb) -> Integer.parseInt(new String((byte[]) bb));
		int size = 2000;

		// Crea un sacco di input, basta un solo output
		// Così abbiamo [size] archi
		MutableString input = new MutableString();
		for (int i = 1; i <= size; i++) input.append("a " + i +"\n");
		MutableString output = new MutableString("a " + (size + 1) + "\n");

		// batch size a uno per stressare i file aperti
		TransactionGraph g = new TransactionGraph(
				new FastByteArrayInputStream(input.toString().getBytes()),
				new FastByteArrayInputStream(output.toString().getBytes()),
				null, -1,
				TransactionGraph.DEFAULT_LABEL_PROTOTYPE, TransactionGraph.DEFAULT_LABEL_MAPPING,
				1_000_000, null, pl);

		assert g.numArcs() == size;

		File f = File.createTempFile("break", "stuff");
		f.deleteOnExit();

		// 40 thread per stressare i file aperti
		BVGraph.storeLabelled(g.arcLabelledBatchGraph, f.toString(), f + ".underlying", 40, pl);

		/*
		GOV3Function<byte[]> addressMap = new GOV3Function.Builder<byte[]>()
				.keys(KEYS.stream().map(String::getBytes).collect(Collectors.toList()))
				.transform(TransformationStrategies.rawByteArray())
				.build();

		for (String key: KEYS) {
			System.out.println(key + " => " + addressMap.getLong(key.getBytes()));
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

		Object2IntFunction<byte[]> addressIntMap = (bb) -> (int) addressMap.getLong(bb);
		Object2LongFunction<? extends CharSequence> stringAddressMap = (s) -> addressMap.getLong(((String) s).getBytes());

		Label prototype = new GammaCodedIntLabel("transaction-id");
		ScatteredLabelledArcsASCIIGraph.LabelMapping labelMapping = (label, bb) -> ((GammaCodedIntLabel) label).value = Integer.parseInt(new String(bb, 1, bb.length - 1));

		System.out.println(new TransactionGraph(inputs, outputs, addressIntMap, (int) addressMap.size64(), prototype, labelMapping, 2, null, pl));
		System.out.println(new ScatteredLabelledArcsASCIIGraph(arcs, stringAddressMap, Charset.defaultCharset(), (int) addressMap.size64(), prototype, labelMapping, null, false, false, 2));
		 */
	}
}
