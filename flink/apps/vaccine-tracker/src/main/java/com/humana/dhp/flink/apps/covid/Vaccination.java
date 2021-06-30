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

package com.humana.dhp.flink.apps.covid;

import lombok.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSink;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSource;
import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;
import org.apache.flink.streaming.connectors.pulsar.internal.JsonDeser;
import org.apache.flink.streaming.connectors.pulsar.internal.JsonSer;
import org.apache.flink.streaming.util.serialization.PulsarDeserializationSchema;
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchema;
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchemaWrapper;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

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
public class Vaccination {
//    private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class Count {
        public String targetGroup;
        public long firstDose;
        public long secondDose;
        public long firstDoseRefused;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    @Getter
    @Setter
    public static class VaccineTracker {
        public String YearWeekISO;
        public long FirstDose;
        public long FirstDoseRefused;
        public long SecondDose;
        public long UnknownDose;
        public long NumberDosesReceived;
        public String Region;
        public String Population;
        public String ReportingCountry;
        public String TargetGroup;
        public String Vaccine;
        public long Denominator;
    }


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000);
//		env.getConfig().setGlobalJobParameters(parameterTool);

        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl("pulsar+ssl://eventbus.npii-aks.dhp-east2us-npe.humana.com:443"); // this field is mandatory.
        clientConf.setAuthPluginClassName("org.apache.pulsar.client.impl.auth.AuthenticationToken");
        clientConf.setAuthParams("token:aaaaaaaaaaaaaaaaaaaaaaaaaa");
        clientConf.setUseTls(true);
        clientConf.setTlsTrustCertsFilePath("/opt/flink/certs/ingressgateway.crt");
//        clientConf.setTlsTrustCertsFilePath(WordCount.class.getResource("/ingressgateway.crt").getPath());
        clientConf.setTlsAllowInsecureConnection(false);
        clientConf.setTlsHostnameVerificationEnable(false);

        String adminServiceUrl = "https://eventbus-admin.npii-aks.dhp-east2us-npe.humana.com:443";
        String inputTopic = "humana/dhp/covid19";
        String outputTopic = "humana/dhp/covid19out";
        int parallelism = 1;

        Properties props = new Properties();
        props.setProperty("topic", inputTopic);
        props.setProperty("partition.discovery.interval-millis", "5000");

        Properties oprops = new Properties();
        oprops.setProperty("topic", outputTopic);
        oprops.setProperty("partition.discovery.interval-millis", "5000");

        PulsarDeserializationSchema<VaccineTracker> pulsarDeserializationSchema = PulsarDeserializationSchema.valueOnly(JsonDeser.of(VaccineTracker.class));
        FlinkPulsarSource<VaccineTracker> source = new FlinkPulsarSource<VaccineTracker>(
                adminServiceUrl,
                clientConf,
                pulsarDeserializationSchema,
                props
        ).setStartFromEarliest();

        DataStream<VaccineTracker> stream = env.addSource(source);
        DataStream<Count> count = stream
                .map((MapFunction<VaccineTracker, Count>) vt -> new Count(vt.TargetGroup, vt.FirstDose, vt.SecondDose, vt.FirstDoseRefused))
                .returns(Count.class)
                .keyBy(c -> c.targetGroup)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .reduce((ReduceFunction<Count>) (c1, c2) ->
                        new Count(c1.targetGroup, c1.firstDose + c2.firstDose, c1.secondDose + c2.secondDose, c1.firstDoseRefused + c2.firstDoseRefused));

        if (null != outputTopic) {
            PulsarSerializationSchema<Count> pulsarSerialization = new PulsarSerializationSchemaWrapper.Builder<>(JsonSer.of(Count.class))
                    .usePojoMode(Count.class, RecordSchemaType.JSON)
                    .setTopicExtractor(dose -> null)
                    .build();

            FlinkPulsarSink<Count> sink = new FlinkPulsarSink<>(
                    adminServiceUrl,
                    Optional.of(outputTopic),
                    clientConf,
                    oprops,
                    pulsarSerialization
            );
            count.addSink(sink);
        } else {
            count.print().setParallelism(parallelism);
        }
        System.out.println("Running ...");
        env.execute("COVID19 Vaccine Tracker");
    }
}
