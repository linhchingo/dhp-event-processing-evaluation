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

package com.humana.dhp.flink.apps;

//import com.sun.org.slf4j.internal.Logger;
//import com.sun.org.slf4j.internal.LoggerFactory;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSink;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSource;


import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;
import org.apache.flink.streaming.connectors.pulsar.internal.JsonSer;
import org.apache.flink.streaming.util.serialization.PulsarDeserializationSchema;
import org.apache.flink.streaming.util.serialization.PulsarDeserializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchema;
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchemaWrapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;
import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class WordCount {
	private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

	@NoArgsConstructor
	@AllArgsConstructor
	@ToString
	public static class Word {
		public String word;
		public long count;
	}


	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		env.enableCheckpointing(5000);
//		env.getConfig().setGlobalJobParameters(parameterTool);

		ClientConfigurationData clientConf = new ClientConfigurationData();
		clientConf.setServiceUrl("pulsar+ssl://eventbus.npii-aks.dhp-east2us-npe.humana.com:443"); // this field is mandatory.
		clientConf.setAuthPluginClassName("org.apache.pulsar.client.impl.auth.AuthenticationToken");
		clientConf.setAuthParams("token:asdfghjklmnbvcxz");
		clientConf.setUseTls(true);
		clientConf.setTlsTrustCertsFilePath("/opt/flink/certs/ingressgateway.crt");
		clientConf.setTlsAllowInsecureConnection(false);
		clientConf.setTlsHostnameVerificationEnable(true);

// 		String brokerServiceUrl = "pulsar+ssl://eventbus.npii-aks.dhp-east2us-npe.humana.com:443";
		String adminServiceUrl = "https://eventbus-admin.npii-aks.dhp-east2us-npe.humana.com:443";
		String inputTopic = "humana/dhp/input";
		String outputTopic = "humana/dhp/output";
		int parallelism = 1;

		Properties props = new Properties();
		props.setProperty("topic", inputTopic);
		props.setProperty("partition.discovery.interval-millis", "5000");

		Properties oprops = new Properties();
		oprops.setProperty("topic", outputTopic);
		oprops.setProperty("partition.discovery.interval-millis", "5000");

		PulsarDeserializationSchema pulsarDeserializationSchema = new PulsarDeserializationSchemaWrapper(new SimpleStringSchema(), DataTypes.STRING());
		FlinkPulsarSource<String> source = new FlinkPulsarSource<String>(
				adminServiceUrl,
				clientConf,
				pulsarDeserializationSchema,
				props
		).setStartFromEarliest();


		DataStream<String> stream = env.addSource(source);
		DataStream<Word> wc = stream
				.flatMap((FlatMapFunction<String, Word>) (line, collector) -> {
					for (String word : line.split("\\s+")) {
						collector.collect(new Word(word, 1));
					}
				})
				.returns(Word.class)
				.keyBy(c -> c.word)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
				.reduce((ReduceFunction<Word>) (c1, c2) ->
						new Word(c1.word, c1.count + c2.count));

		if (null != outputTopic) {
			PulsarSerializationSchema<Word> pulsarSerialization = new PulsarSerializationSchemaWrapper.Builder<>(JsonSer.of(Word.class))
					.usePojoMode(Word.class, RecordSchemaType.JSON)
					.setTopicExtractor(word -> null)
					.build();

			FlinkPulsarSink<Word> sink = new FlinkPulsarSink<>(
					adminServiceUrl,
					Optional.of(outputTopic),
					clientConf,
					oprops,
					pulsarSerialization
			);
			wc.addSink(sink);
		} else {
			wc.print().setParallelism(parallelism);
		}
//		wc.writeAsText("wasbs://example@flinkstorage1.blob.core.windows.net/output/out.txt", FileSystem.WriteMode.OVERWRITE);
		System.out.println("Running ...");
		env.execute("Flink Pulsar");
	}


}
