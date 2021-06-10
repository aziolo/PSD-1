/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;


public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//read data from file
		String csvFilePath = "/home/shadowadu/Flink/psd1/wallet.csv";
		Path filePath = new Path(csvFilePath);
		TextInputFormat inputFormat = new TextInputFormat(filePath);
		DataStreamSource<String> inputStream = env.readFile(inputFormat, csvFilePath);
		DataStream<String> filteredStream = inputStream.filter(new titleFilter());
		DataStream<rateOfReturn> mappedStream = filteredStream.map(new parseRow());
		WindowedStream<rateOfReturn, Tuple, GlobalWindow> windowedStream = mappedStream.keyBy("key").countWindow(30,1).trigger(new fullTrigger());
        //calculating statistics
		DataStream<Tuple4<Integer, String, String, Double>> alerts = windowedStream.apply(new controlStream());

		alerts.writeAsCsv("/home/shadowadu/Flink/psd1/alerts10.csv", FileSystem.WriteMode.OVERWRITE);

		env.execute("Flink Streaming Java API Skeleton");


	}
	public static class fullTrigger extends Trigger {
		int count = 0;

		@Override
		public TriggerResult onElement(Object o, long l, Window window, TriggerContext triggerContext) throws Exception {
			count ++;
			if (count < 30) return TriggerResult.CONTINUE;
			else return TriggerResult.FIRE;
		}

		@Override
		public TriggerResult onProcessingTime(long l, Window window, TriggerContext triggerContext) throws Exception {
			return null;
		}

		@Override
		public TriggerResult onEventTime(long l, Window window, TriggerContext triggerContext) throws Exception {
			return null;
		}

		@Override
		public void clear(Window window, TriggerContext triggerContext) throws Exception {

		}

	}

	public static class controlStream extends RichWindowFunction<rateOfReturn, Tuple4<Integer, String, String, Double>, Tuple, GlobalWindow> {
		private transient ValueState<Integer> iterator;
		List<Double> sortedAsset1 = new LinkedList<>();
		List<Double> sortedAsset2 = new LinkedList<>();
		List<Double> sortedAsset3 = new LinkedList<>();
		List<Double> sortedAsset4 = new LinkedList<>();
		List<Double> sortedAsset5 = new LinkedList<>();
		List<Double> sortedAsset6 = new LinkedList<>();
		List<Double> sortedWallet = new LinkedList<>();

		List<Double> averages = new LinkedList<>();
		List<Double> medians = new LinkedList<>();
		List<Double> quantile10 = new LinkedList<>();
		List<Double> averageMin10 = new LinkedList<>();
		List<Double> securityDeviation = new LinkedList<>();
		List<Double> securityGini = new LinkedList<>();
		transient rateOfReturn oldSample = new rateOfReturn();
		transient rateOfReturn newSample = new rateOfReturn();
		List<Integer> oldIndexes = new LinkedList<>();
		double k = 1.0/60.0;
		double g = 1.0/Math.pow(30,2);
		double l =31.0/30.0;

		List<Tuple4<Integer, String, String, Double>> exceeds= new LinkedList<>();
		List<Double> AVERAGE = List.of(3.840929E-05, 0.0001004826, 3.285604E-05, 4.406543E-05, -3.06396E-05, -2.394491E-05, 5.661496E-06);
		List<Double> MEDIAN = List.of(0.0001198415, 0.000196448, 6.270111E-06, 9.744538E-05, -7.461998E-05, -0.0001010394, 4.464863E-06);
		List<Double> QUANTILE10 = List.of(-0.7971767, -0.07992966,-0.07996471, -0.07999294, -0.08003861, -0.0799577, -0.005221751);
		List<Double> AVERAGE10 = List.of(-0.08987466,-0.08996232,-0.089969,-0.0900202,-0.09002384,-0.08999463,-0.007004326);
		List<Double> DEV_SECURITY = List.of(-0.02488319, -0.02489892, -0.02493481, -0.02493518, -0.02503062, -0.2501139, -0.001615676);
		List<Double> GINI_SECURITY = List.of(0.06648535,0.06666021,0.0666026,0.06663066,0.06667714,0.06664527,0.004562588);
		List<String> titles = List.of("average", "median", "quantile 10%", "average min 10%", "security deviation", "security gini");
		List<String> names = List.of("asset1", "asset2", "asset3", "asset4", "asset5", "asset6", "wallet");


		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<Integer>("iterator", TypeInformation.of(new TypeHint<Integer>() {}), 0);
			 iterator = getRuntimeContext().getState(descriptor);
		}

		@Override
		public void apply(Tuple key, GlobalWindow global, Iterable<rateOfReturn> iterable, Collector<Tuple4<Integer, String, String, Double>> collector) throws Exception {
			int currentIterator = iterator.value();
			currentIterator +=1;
			iterator.update(currentIterator);
			List<Double> asset1 = new LinkedList<>();
			List<Double> asset2 = new LinkedList<>();
			List<Double> asset3 = new LinkedList<>();
			List<Double> asset4 = new LinkedList<>();
			List<Double> asset5 = new LinkedList<>();
			List<Double> asset6 = new LinkedList<>();
			List<Double> wallet = new LinkedList<>();

			int index = 0;
			for (rateOfReturn rate : iterable){
				asset1.add(rate.asset1);
				asset2.add(rate.asset2);
				asset3.add(rate.asset3);
				asset4.add(rate.asset4);
				asset5.add(rate.asset5);
				asset6.add(rate.asset6);
				wallet.add(rate.wallet);
				if (index == 0){
					newSample = rate;
				}
				if (index == 29){
					oldSample = rate;
				}
				index ++;
			}
			if(iterator.value() == 1){
				sortFirstWindow(asset1, asset2, asset3, asset4, asset5, asset6, wallet);
				calculateFirstStatistics();
			}
			// for next windows
			if (iterator.value() > 1){
				updateSortedLists();
				updateStatisticForNewWindow();
			}
			collectAlerts(collector);

		}

		private void collectAlerts(Collector<Tuple4<Integer, String, String, Double>> collector) throws IOException {
		    double point = 0.1;
			for (int i=0; i < 7; i++){
				if(averages.get(i) < AVERAGE.get(i)){
					double distance_avg = (AVERAGE.get(i) - averages.get(i)) / (1 + AVERAGE.get(i));
					if (distance_avg >= point) collector.collect(Tuple4.of(iterator.value(), names.get(i), titles.get(0), distance_avg));
				}
				if(medians.get(i) < MEDIAN.get(i)){
					double distance_med = (MEDIAN.get(i) - medians.get(i)) / (1 + MEDIAN.get(i));
					if (distance_med >= point) collector.collect(Tuple4.of(iterator.value(), names.get(i), titles.get(1), distance_med));
				}
				if(quantile10.get(i) < QUANTILE10.get(i)){
					double distance_quant = (QUANTILE10.get(i) - quantile10.get(i)) / (1 + QUANTILE10.get(i));
					if (distance_quant >= point) collector.collect(Tuple4.of(iterator.value(), names.get(i), titles.get(2), distance_quant));
				}
				if(averageMin10.get(i) < AVERAGE10.get(i)){
					double distance_avg10 = (AVERAGE10.get(i) - averageMin10.get(i)) / (1 + AVERAGE10.get(i));
					if (distance_avg10 >= point) collector.collect(Tuple4.of(iterator.value(), names.get(i), titles.get(3), distance_avg10));
				}
				if(securityDeviation.get(i) < DEV_SECURITY.get(i)){
					double distance_dev = (DEV_SECURITY.get(i) - securityDeviation.get(i)) / (1 + DEV_SECURITY.get(i));
					if (distance_dev >= point) collector.collect(Tuple4.of(iterator.value(), names.get(i), titles.get(4), distance_dev));
				}
				if(securityGini.get(i) < GINI_SECURITY.get(i)){
					double distance_gini = (GINI_SECURITY.get(i) - securityGini.get(i)) / (1 + GINI_SECURITY.get(i));
					if (distance_gini >= point) collector.collect(Tuple4.of(iterator.value(), names.get(i), titles.get(5), distance_gini));
				}
			}
		}

		private void sortFirstWindow(List<Double> asset1, List<Double> asset2, List<Double> asset3, List<Double> asset4, List<Double> asset5, List<Double> asset6, List<Double> wallet) {
            Collections.sort(asset1);
            sortedAsset1.addAll(asset1);
            Collections.sort(asset2);
            sortedAsset2.addAll(asset2);
            Collections.sort(asset3);
            sortedAsset3.addAll(asset3);
            Collections.sort(asset4);
            sortedAsset4.addAll(asset4);
            Collections.sort(asset5);
            sortedAsset5.addAll(asset5);
            Collections.sort(asset6);
            sortedAsset6.addAll(asset6);
            Collections.sort(wallet);
            sortedWallet.addAll(wallet);
            int k1 = sortedAsset1.indexOf(oldSample.asset1);
            int k2 = sortedAsset2.indexOf(oldSample.asset2);
            int k3 = sortedAsset3.indexOf(oldSample.asset3);
            int k4 = sortedAsset4.indexOf(oldSample.asset4);
            int k5 = sortedAsset5.indexOf(oldSample.asset5);
            int k6 = sortedAsset6.indexOf(oldSample.asset6);
            int k7 = sortedWallet.indexOf(oldSample.wallet);
            oldIndexes.add(k1);
            oldIndexes.add(k2);
            oldIndexes.add(k3);
            oldIndexes.add(k4);
            oldIndexes.add(k5);
            oldIndexes.add(k6);
            oldIndexes.add(k7);
        }

		private void updateSortedLists() {
			sortedAsset1.remove(oldIndexes.get(0).intValue());
			sortedAsset2.remove(oldIndexes.get(1).intValue());
			sortedAsset3.remove(oldIndexes.get(2).intValue());
			sortedAsset4.remove(oldIndexes.get(3).intValue());
			sortedAsset5.remove(oldIndexes.get(4).intValue());
			sortedAsset6.remove(oldIndexes.get(5).intValue());
			sortedWallet.remove(oldIndexes.get(6).intValue());
			// przypadek gdy wymieniamy pierwszy element
			if (newSample.asset1 <= sortedAsset1.get(0)){
				sortedAsset1.add(0, newSample.asset1);
				oldIndexes.set(0,0);
			}
			if (newSample.asset2 <= sortedAsset2.get(0)){
				sortedAsset2.add(0, newSample.asset2);
				oldIndexes.set(1,0);
			}
			if (newSample.asset3 <= sortedAsset3.get(0)){
				sortedAsset3.add(0, newSample.asset3);
				oldIndexes.set(2,0);
			}
			if (newSample.asset4 <= sortedAsset4.get(0)){
				sortedAsset4.add(0, newSample.asset4);
				oldIndexes.set(3,0);
			}
			if (newSample.asset5 <= sortedAsset5.get(0)){
				sortedAsset5.add(0, newSample.asset5);
				oldIndexes.set(4,0);
			}
			if (newSample.asset6 <= sortedAsset6.get(0)){
				sortedAsset6.add(0, newSample.asset6);
				oldIndexes.set(5,0);
			}
			if (newSample.wallet <= sortedWallet.get(0)){
				sortedWallet.add(0, newSample.wallet);
				oldIndexes.set(6,0);
			}
			// przypadek, gdy nowy element jest wiekszy od ostatniego
			if (newSample.asset1 > sortedAsset1.get(28)){
				sortedAsset1.add(newSample.asset1);
				oldIndexes.set(0,29);
			}
			if (newSample.asset2 > sortedAsset2.get(28)){
				sortedAsset2.add(29, newSample.asset2);
				oldIndexes.set(1,29);
			}
			if (newSample.asset3 > sortedAsset3.get(28)){
				sortedAsset3.add(29, newSample.asset3);
				oldIndexes.set(2,29);
			}
			if (newSample.asset4 > sortedAsset4.get(28)){
				sortedAsset4.add(29, newSample.asset4);
				oldIndexes.set(3,29);
			}
			if (newSample.asset5 > sortedAsset5.get(28)){
				sortedAsset5.add(29, newSample.asset5);
				oldIndexes.set(4,29);
			}
			if (newSample.asset6 > sortedAsset6.get(28)){
				sortedAsset6.add(29, newSample.asset6);
				oldIndexes.set(5,29);
			}
			if (newSample.wallet > sortedWallet.get(28)){
				sortedWallet.add(29, newSample.wallet);
				oldIndexes.set(6,29);
			}
			//przypadki porsednie - trzeba iterowac aby znalezc miejsce w tabeli
			for (int i=0;  i < 28; i++){
				if (newSample.asset1 > sortedAsset1.get(i) && newSample.asset1 <= sortedAsset1.get(i+1)){
					sortedAsset1.add(i+1, newSample.asset1);
					oldIndexes.set(0,i+1);
				}
				if (newSample.asset2 > sortedAsset2.get(i) && newSample.asset2 <= sortedAsset2.get(i+1)){
					sortedAsset2.add(i+1, newSample.asset2);
					oldIndexes.set(1,i+1);
				}
				if (newSample.asset3 > sortedAsset3.get(i) && newSample.asset3 <= sortedAsset3.get(i+1)){
					sortedAsset3.add(i+1, newSample.asset3);
					oldIndexes.set(2,i+1);
				}
				if (newSample.asset4 > sortedAsset4.get(i) && newSample.asset4 <= sortedAsset4.get(i+1)){
					sortedAsset4.add(i+1, newSample.asset4);
					oldIndexes.set(3,i+1);
				}
				if (newSample.asset5 > sortedAsset5.get(i) && newSample.asset5 <= sortedAsset5.get(i+1)){
					sortedAsset5.add(i+1, newSample.asset5);
					oldIndexes.set(4,i+1);
				}
				if (newSample.asset6 > sortedAsset6.get(i) && newSample.asset6 <= sortedAsset6.get(i+1)){
					sortedAsset6.add(i+1, newSample.asset6);
					oldIndexes.set(5,i+1);
				}
				if (newSample.wallet > sortedWallet.get(i) && newSample.wallet <= sortedWallet.get(i+1)){
					sortedWallet.add(i+1, newSample.wallet);
					oldIndexes.set(6,i+1);
				}
			}
		}

        private void calculateFirstStatistics() {
            averages.add( sortedAsset1.stream().mapToDouble(a -> a).average().getAsDouble());
            averages.add( sortedAsset2.stream().mapToDouble(a -> a).average().getAsDouble());
            averages.add( sortedAsset3.stream().mapToDouble(a -> a).average().getAsDouble());
            averages.add( sortedAsset4.stream().mapToDouble(a -> a).average().getAsDouble());
            averages.add( sortedAsset5.stream().mapToDouble(a -> a).average().getAsDouble());
            averages.add( sortedAsset6.stream().mapToDouble(a -> a).average().getAsDouble());
            averages.add( sortedWallet.stream().mapToDouble(a -> a).average().getAsDouble());
            medians.add(((sortedAsset1.get(14) + sortedAsset1.get(15))/2));
            medians.add(((sortedAsset2.get(14) + sortedAsset2.get(15))/2));
            medians.add(((sortedAsset3.get(14) + sortedAsset3.get(15))/2));
            medians.add(((sortedAsset4.get(14) + sortedAsset4.get(15))/2));
            medians.add(((sortedAsset5.get(14) + sortedAsset5.get(15))/2));
            medians.add(((sortedAsset6.get(14) + sortedAsset6.get(15))/2));
            medians.add(((sortedWallet.get(14) + sortedWallet.get(15))/2));
            quantile10.add(quantile(sortedAsset1));
            quantile10.add(quantile(sortedAsset2));
            quantile10.add(quantile(sortedAsset3));
            quantile10.add(quantile(sortedAsset4));
            quantile10.add(quantile(sortedAsset5));
            quantile10.add(quantile(sortedAsset6));
            quantile10.add(quantile(sortedWallet));
            averageMin10.add(sortedAsset1.subList(0,2).stream().mapToDouble(a -> a).average().getAsDouble());
            averageMin10.add(sortedAsset2.subList(0,2).stream().mapToDouble(a -> a).average().getAsDouble());
            averageMin10.add(sortedAsset3.subList(0,2).stream().mapToDouble(a -> a).average().getAsDouble());
            averageMin10.add(sortedAsset4.subList(0,2).stream().mapToDouble(a -> a).average().getAsDouble());
            averageMin10.add(sortedAsset5.subList(0,2).stream().mapToDouble(a -> a).average().getAsDouble());
            averageMin10.add(sortedAsset6.subList(0,2).stream().mapToDouble(a -> a).average().getAsDouble());
            averageMin10.add(sortedWallet.subList(0,2).stream().mapToDouble(a -> a).average().getAsDouble());
            double dev1 = 0.0;
            double dev2 = 0.0;
            double dev3 = 0.0;
            double dev4 = 0.0;
            double dev5 = 0.0;
            double dev6 = 0.0;
            double devW = 0.0;
            double gini1 = 0.0;
            double gini2 = 0.0;
            double gini3 = 0.0;
            double gini4 = 0.0;
            double gini5 = 0.0;
            double gini6 = 0.0;
            double giniW = 0.0;
            for (int i=0;  i< 30; i++){
                dev1 += Math.abs(averages.get(0) - sortedAsset1.get(i));
                dev2 += Math.abs(averages.get(1) - sortedAsset2.get(i));
                dev3 += Math.abs(averages.get(2) - sortedAsset3.get(i));
                dev4 += Math.abs(averages.get(3) - sortedAsset4.get(i));
                dev5 += Math.abs(averages.get(4) - sortedAsset5.get(i));
                dev6 += Math.abs(averages.get(5) - sortedAsset6.get(i));
                devW += Math.abs(averages.get(6) - sortedWallet.get(i));

                gini1 += ((2*i) - 29) * sortedAsset1.get(i);
                gini2 += ((2*i) - 29) * sortedAsset2.get(i);
                gini3 += ((2*i) - 29) * sortedAsset3.get(i);
                gini4 += ((2*i) - 29) * sortedAsset4.get(i);
                gini5 += ((2*i) - 29) * sortedAsset5.get(i);
                gini6 += ((2*i) - 29) * sortedAsset6.get(i);
                giniW += ((2*i) - 29) * sortedWallet.get(i);
            }
            securityDeviation.add(averages.get(0) - (k * dev1));
            securityDeviation.add(averages.get(1) - (k * dev2));
            securityDeviation.add(averages.get(2) - (k * dev3));
            securityDeviation.add(averages.get(3) - (k * dev4));
            securityDeviation.add(averages.get(4) - (k * dev5));
            securityDeviation.add(averages.get(5) - (k * dev6));
            securityDeviation.add(averages.get(6) - (k * devW));

            securityGini.add(gini1 * g);
            securityGini.add(gini2 * g);
            securityGini.add(gini3 * g);
            securityGini.add(gini4 * g);
            securityGini.add(gini5 * g);
            securityGini.add(gini6 * g);
            securityGini.add(giniW * g);

            securityGini.add((gini1 * g) - l);
            securityGini.add((gini2 * g) - l);
            securityGini.add((gini3 * g) - l);
            securityGini.add((gini4 * g) - l);
            securityGini.add((gini5 * g) - l);
            securityGini.add((gini6 * g) - l);
            securityGini.add((giniW * g) - l);
        }

		private void updateStatisticForNewWindow() {
			averages.set(0, (averages.get(0) - (oldSample.asset1/30) + (newSample.asset1/30)));
			averages.set(1, (averages.get(1) - (oldSample.asset2/30) + (newSample.asset2/30)));
			averages.set(2, (averages.get(2) - (oldSample.asset3/30) + (newSample.asset3/30)));
			averages.set(3, (averages.get(3) - (oldSample.asset4/30) + (newSample.asset4/30)));
			averages.set(4, (averages.get(4) - (oldSample.asset5/30) + (newSample.asset5/30)));
			averages.set(5, (averages.get(5) - (oldSample.asset6/30) + (newSample.asset6/30)));
			averages.set(6, (averages.get(6) - (oldSample.wallet/30) + (newSample.wallet/30)));
			medians.set(0, ((sortedAsset1.get(14) + sortedAsset1.get(15))/2));
			medians.set(1, ((sortedAsset2.get(14) + sortedAsset2.get(15))/2));
			medians.set(2, ((sortedAsset3.get(14) + sortedAsset3.get(15))/2));
			medians.set(3, ((sortedAsset4.get(14) + sortedAsset4.get(15))/2));
			medians.set(4, ((sortedAsset5.get(14) + sortedAsset5.get(15))/2));
			medians.set(5, ((sortedAsset6.get(14) + sortedAsset6.get(15))/2));
			medians.set(6, ((sortedWallet.get(14) + sortedWallet.get(15))/2));
			quantile10.set(0, quantile(sortedAsset1));
			quantile10.set(1, quantile(sortedAsset2));
			quantile10.set(2, quantile(sortedAsset3));
			quantile10.set(3, quantile(sortedAsset4));
			quantile10.set(4, quantile(sortedAsset5));
			quantile10.set(5, quantile(sortedAsset6));
			quantile10.set(6, quantile(sortedWallet));
			averageMin10.set(0, sortedAsset1.subList(0,2).stream().mapToDouble(a -> a).average().getAsDouble());
			averageMin10.set(1, sortedAsset2.subList(0,2).stream().mapToDouble(a -> a).average().getAsDouble());
			averageMin10.set(2, sortedAsset3.subList(0,2).stream().mapToDouble(a -> a).average().getAsDouble());
			averageMin10.set(3, sortedAsset4.subList(0,2).stream().mapToDouble(a -> a).average().getAsDouble());
			averageMin10.set(4, sortedAsset5.subList(0,2).stream().mapToDouble(a -> a).average().getAsDouble());
			averageMin10.set(5, sortedAsset6.subList(0,2).stream().mapToDouble(a -> a).average().getAsDouble());
			averageMin10.set(6, sortedWallet.subList(0,2).stream().mapToDouble(a -> a).average().getAsDouble());
			double dev1 = 0.0;
			double dev2 = 0.0;
			double dev3 = 0.0;
			double dev4 = 0.0;
			double dev5 = 0.0;
			double dev6 = 0.0;
			double devW = 0.0;
			double gini1 = 0.0;
			double gini2 = 0.0;
			double gini3 = 0.0;
			double gini4 = 0.0;
			double gini5 = 0.0;
			double gini6 = 0.0;
			double giniW = 0.0;
			for (int i=0;  i< 30; i++){
				dev1 += Math.abs(averages.get(0) - sortedAsset1.get(i));
				dev2 += Math.abs(averages.get(1) - sortedAsset2.get(i));
				dev3 += Math.abs(averages.get(2) - sortedAsset3.get(i));
				dev4 += Math.abs(averages.get(3) - sortedAsset4.get(i));
				dev5 += Math.abs(averages.get(4) - sortedAsset5.get(i));
				dev6 += Math.abs(averages.get(5) - sortedAsset6.get(i));
				devW += Math.abs(averages.get(6) - sortedWallet.get(i));

				gini1 += ((2*i) - 29) * sortedAsset1.get(i);
				gini2 += ((2*i) - 29) * sortedAsset2.get(i);
				gini3 += ((2*i) - 29) * sortedAsset3.get(i);
				gini4 += ((2*i) - 29) * sortedAsset4.get(i);
				gini5 += ((2*i) - 29) * sortedAsset5.get(i);
				gini6 += ((2*i) - 29) * sortedAsset6.get(i);
				giniW += ((2*i) - 29) * sortedWallet.get(i);

			}
			securityDeviation.set(0, averages.get(0) - (k * dev1));
			securityDeviation.set(1, averages.get(1) - (k * dev2));
			securityDeviation.set(2, averages.get(2) - (k * dev3));
			securityDeviation.set(3, averages.get(3) - (k * dev4));
			securityDeviation.set(4, averages.get(4) - (k * dev5));
			securityDeviation.set(5, averages.get(5) - (k * dev6));
			securityDeviation.set(6, averages.get(6) - (k * devW));

			securityGini.set(0, (gini1 * g));
			securityGini.set(1, (gini2 * g));
			securityGini.set(2, (gini3 * g));
			securityGini.set(3, (gini4 * g));
			securityGini.set(4, (gini5 * g));
			securityGini.set(5, (gini6 * g));
			securityGini.set(6, (giniW * g));
		}

		public Double quantile(List<Double> list){
			double cut = (0.1 * 29);
			int index = (int) cut;
			double diff = cut- index;
			return list.get(index) + ((list.get(index+1) - list.get(index)) * diff);
		}
	}

	public static class parseRow implements MapFunction<String, rateOfReturn>{

		@Override
		public rateOfReturn map(String input) throws Exception {
				String [] rowData = input.split(",");
				return new rateOfReturn(rowData[1],rowData[2],rowData[3],rowData[4],rowData[5],rowData[6],rowData[7]);
		}
	}

	public static class rateOfReturn{
		public Integer key = 0;
		public Double asset1;
		public Double asset2;
		public Double asset3;
		public Double asset4;
		public Double asset5;
		public Double asset6;
		public Double wallet;

		public rateOfReturn(){
        };

		public rateOfReturn(String asset1, String asset2, String asset3, String asset4, String asset5, String asset6, String wallet){
			this.key = 0;
			this.asset1 = Double.parseDouble(asset1);
			this.asset2 = Double.parseDouble(asset2);
			this.asset3 = Double.parseDouble(asset3);
			this.asset4 = Double.parseDouble(asset4);
			this.asset5 = Double.parseDouble(asset5);
			this.asset6 = Double.parseDouble(asset6);
			this.wallet = Double.parseDouble(wallet);
		}
		public String toString(){
			return "wallet: " + wallet.toString() + ", asset1: " + asset1.toString() + ", asset2: " + asset2.toString() + ", asset3: " + asset3.toString() +", asset4: " + asset4.toString() + ", asset5: " + asset5.toString() +", asset6: " + asset6.toString();
		}
	}

	public static class titleFilter implements FilterFunction<String>{
          @Override
        public boolean filter(String line) throws Exception {
            if (line.contains("V1")) return false;
            if (line.contains("V2")) return false;
            if (line.contains("V3")) return false;
            if (line.contains("V4")) return false;
            if (line.contains("V5")) return false;
            if (line.contains("V6")) return false;
            if (line.contains("wallet_column")) return false;
            else return true;
        }
    }

}
