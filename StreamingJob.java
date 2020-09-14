package org.myorg.project;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.myorg.project.comparison.HDRComparer;
import org.myorg.project.comparison.LogInOutComparer;
import org.myorg.project.comparison.XpComparer;
import org.myorg.project.elasticSearchSinkFunction.BasicElasticSearchInserter;
import org.myorg.project.elasticSearchSinkFunction.PlayerKDRInserter;
import org.myorg.project.onlinePrediction.ConsensusTrendPrediction;
import org.myorg.project.onlinePrediction.IterativeMeanPrediction;
import org.myorg.project.aggregateStreaming.StreamAggregateEventExperience;
import org.myorg.project.aggregateStreaming.StreamAggregateEventHeadshotRatio;
import org.myorg.project.filterStreaming.*;
import org.myorg.project.mapStreaming.StreamMapEventJsonPayload;
import org.myorg.project.mapStreaming.*;
import org.myorg.project.planetside2.Planetside;
import org.myorg.project.reduceStream.StreamReduceEventDeathKDR;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/*
    StreamingJob - Main class for the streaming computations
    Content:
    - SETUP
    - Compute 5 METRICS
    - COMPARE STREAMING RESULTS WITH BATCH RESULTS
    - PREDICTION
    - SINK TO ELASTICSEARCH
*/


public class StreamingJob {

	public static void main(String[] args) throws Exception {

        //******************
        // SETUP
        //******************

        // time window seconds
        int timeSeconds = 30;

		// set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // basic data stream processing, connect to localhost for websocket support
        DataStream<String> rawEvents = env.socketTextStream("localhost",1337);
        DataStream<String> rawPayloadEvents = rawEvents.filter( new StreamFilterEventPayload());
        DataStream<JsonNode> jsonPayloadEvents = rawPayloadEvents.map( new StreamMapEventJsonPayload());

        //******************
        // Compute 5 METRICS
        //******************

        //******************
        //        1
        //******************
        // Compute the number of logins/logouts for each world
		// format: event,world_id,count
		// goal: track the logInOut traffic
		DataStream<Tuple3< String, Integer, Integer>> logInOut = jsonPayloadEvents
				.filter( new StreamFilterEventLogInOut())
				.map(new StreamMapEventLogInOut())
				.keyBy(0,1)
				.timeWindow(Time.seconds(timeSeconds))
				.sum(2);

        //******************
        //        2
        //******************
		// Compute headshots/deaths ratio over all worlds
		// format: num headshots, num deaths, ratio
		// goal: track player headshot accuracy
		DataStream<Tuple3<Integer,Integer,Double>> headshotDeathRatio = jsonPayloadEvents
				.filter( new StreamFilterEventDeath())
				.map( new StreamMapEventHeadshot())
				.keyBy(0)
				.timeWindow(Time.seconds(timeSeconds))
				.aggregate( new StreamAggregateEventHeadshotRatio());

        //******************
        //        3
        //******************
		// Compute kills/deaths ratio for every active player
		// format: character, kd ratio
		// goal: track player skill
		DataStream<Tuple2<String, Double>> KDR = jsonPayloadEvents
				.filter( new StreamFilterEventDeath())
				.flatMap( new StreamFlatmapEventDeathKDR())
				.keyBy(0)
				.timeWindow(Time.seconds(timeSeconds))
				.reduce( new StreamReduceEventDeathKDR())
				.keyBy(0)
				.map( new StreamMapEventDeathKDR());

        //******************
        //        4
        //******************
		// Compute the region with max player activity for each world (real time)
		// format: event, region id, world id, number of participating players
		// goal: find the region which has the largest fight at the moment
		DataStream<Tuple4<String, String, Integer, Integer>> facilityActivity = jsonPayloadEvents
				.filter( new StreamFilterEventFacility())
				.map( new StreamMapEventFacility())
				.keyBy(0,1,2)
                .timeWindow(Time.seconds(timeSeconds))
				.sum(3)
				.keyBy(2)
				.timeWindow(Time.milliseconds(500))
				.maxBy(3);

        //******************
        //        5
        //******************
		// Compute the average experience gain for each world (for a given time interval)
		// format: world id, active players (players who earn xp in the interval), average xp gain
		// goal: try to look if there are more infantry or vehicle fights at the moment and overall player performance
		DataStream<Tuple3<Integer, Integer, Double>> averageXpGain = jsonPayloadEvents
				.filter( new StreamFilterEventExperience())
				.map( new StreamMapEventExperience())
				.keyBy(0,1)
				.timeWindow(Time.seconds(timeSeconds))
				.sum(2)
				.keyBy(0)
				.timeWindow(Time.milliseconds(500))
				.aggregate( new StreamAggregateEventExperience());


        //**********************************************
        // COMPARE STREAMING RESULTS WITH BATCH RESULTS
        //**********************************************
        // read in the batch results and union them in the same datastream as the streaming metric
        // basically just subtract the batch value with the streaming values to "compare" them

        // COMPARE logInOut
        DataStream<String> rawBatchLogInOut = env.readTextFile("batch_processing_result/LogInOut");
        DataStream<Tuple4<Integer, String, Integer, Double>> batchLogInOut = rawBatchLogInOut
                .map(new MapFunction<String, Tuple4<Integer, String, Integer, Double>>() {
                         @Override
                         public Tuple4<Integer, String, Integer, Double> map(String s) throws Exception {
                             String[] fields = s.split(",");
                             return new Tuple4<>(
                                     1,
                                     fields[0],
                                     Integer.parseInt(fields[1]),
                                     Double.parseDouble(fields[2])
                             );
                         }
                     });
        DataStream<Tuple4<Integer, String, Integer, Double>> streamLogInOut = logInOut
                .map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple4<Integer, String, Integer, Double>>() {
                    @Override
                    public Tuple4<Integer, String, Integer, Double> map(Tuple3<String, Integer, Integer> t) throws Exception {
                        return new Tuple4<>(
                                0,
                                t.f0,
                                t.f1,
                                (double)t.f2
                        );
                    }
                });

        DataStream<Tuple3<String, Integer, Double>> compareLogInOut =  batchLogInOut.union(streamLogInOut)
                .keyBy(1,2)
                .flatMap( new LogInOutComparer());

        // COMPARE HDR

        DataStream<String> rawBatchHDR = env.readTextFile("batch_processing_result/HDR");
        DataStream<Tuple3<String, Integer, Double>> batchHDR = rawBatchHDR
                .map(new MapFunction<String, Tuple3<String, Integer, Double>>() {
                    @Override
                    public Tuple3<String, Integer, Double> map(String s) throws Exception {
                        String[] fields = s.split(",");
                        return new Tuple3<>(
                                "key",
                                1,
                                Double.parseDouble(fields[2])
                        );
                    }
                });

        DataStream<Tuple3<String, Integer, Double>> streamHDR = headshotDeathRatio
                .map(new MapFunction<Tuple3<Integer, Integer, Double>, Tuple3<String, Integer, Double>>() {
                    @Override
                    public Tuple3<String, Integer, Double> map(Tuple3<Integer, Integer, Double> t) throws Exception {
                        return new Tuple3<>(
                                "key",
                                0,
                                t.f2
                        );
                    }
                });

        DataStream<Tuple1<Double>> compareHDR =  batchHDR.union(streamHDR)
                .keyBy(0)
                .flatMap( new HDRComparer());


        // COMPARE Xp
        DataStream<String> rawBatchXp = env.readTextFile("batch_processing_result/XP");
        DataStream<Tuple3<Integer,Integer, Double>> batchXp = rawBatchXp
                .map(new MapFunction<String, Tuple3<Integer, Integer, Double>>() {
                    @Override
                    public Tuple3<Integer, Integer, Double> map(String s) throws Exception {
                        String[] fields = s.split(",");
                        return new Tuple3<>(
                                1,
                                Integer.parseInt(fields[0]),
                                Double.parseDouble(fields[1])
                        );
                    }
                });

        DataStream<Tuple3<Integer,Integer, Double>> streamXp = averageXpGain
                .map(new MapFunction<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Integer, Double>>() {
                    @Override
                    public Tuple3<Integer, Integer, Double> map(Tuple3<Integer, Integer, Double> t) throws Exception {
                        return new Tuple3<>(
                                0,
                                t.f0,
                                t.f2
                        );
                    }
                });

        DataStream<Tuple2<String, Double>> compareXp =  batchXp.union(streamXp)
                .keyBy(1)
                .flatMap( new XpComparer());

        // map all comparisons to elasticSearch format

        DataStream<Tuple2<String, Double>> elasticCompareLogInOut = compareLogInOut
                .map(new MapFunction<Tuple3<String, Integer, Double>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple3<String, Integer, Double> t) throws Exception {
                        return new Tuple2<>(
                                "Compare " + t.f0 +" "+ Planetside.serverIdToName(t.f1),
                                t.f2
                        );
                    }
                });

        DataStream<Tuple2<String, Double>> elasticCompareHDR= compareHDR
                .map(new MapFunction<Tuple1<Double>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple1<Double> t) throws Exception {
                        return new Tuple2<>(
                                "CompareHDR",
                                t.f0
                        );
                    }
                });

        DataStream<Tuple2<String, Double>> elasticCompareXp= compareXp
                .map(new MapFunction<Tuple2<String, Double>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple2<String, Double> t) throws Exception {
                        return new Tuple2<>(
                                "CompareXP " + Planetside.serverIdToName(Integer.parseInt(t.f0)),
                                t.f1
                        );
                    }
                });


        //************
        // PREDICTION
        //************
        // do value prediction with an online iterative mean model
        // & do trend prediction with an online consensus model

        // iterative mean model

        // PREDICT XP gain - iterative mean model
        DataStream < Tuple2 < String, Double >> predictedXpGain = averageXpGain
                .map(new MapFunction<Tuple3<Integer, Integer, Double>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple3<Integer, Integer, Double> t) throws Exception {
                        return new Tuple2<>("PredictXP" + " " + Planetside.serverIdToName(t.f0), t.f2);
                    }
                })
                .keyBy(0)
                .map(new IterativeMeanPrediction());

        // PREDICT HDR - iterative mean model
        DataStream<Tuple2<String, Double>> predictedHDR = headshotDeathRatio
                .map(new MapFunction<Tuple3<Integer, Integer, Double>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple3<Integer, Integer, Double> t) throws Exception {
                        return new Tuple2<>("PredictHDR", t.f2);
                    }
                })
                .keyBy(0)
                .map( new IterativeMeanPrediction());

        // PREDICT logInOuts - iterative mean model
        DataStream<Tuple2<String, Double>> predictedLogInOut = logInOut
                .map(new MapFunction<Tuple3< String, Integer, Integer>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple3< String, Integer, Integer> t) throws Exception {
                        return new Tuple2<>("Prediction " +String.valueOf(t.f0) +" "+ Planetside.serverIdToName(t.f1), (double)t.f2);
                    }
                })
                .keyBy(0)
                .map( new IterativeMeanPrediction());

        // consensus model

        // CONSENSUS CompareLogInOut - consensus model
        DataStream<Tuple2<String, Double>> elasticPredictTrendCompareLogInOut = elasticCompareLogInOut
                .map(new MapFunction<Tuple2<String, Double>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple2<String, Double> t) throws Exception {
                        t.f0 = "PredictTrend" + t.f0;
                        return t;
                    }
                })
                .keyBy(0)
                .map( new ConsensusTrendPrediction());

        // CONSENSUS CompareHDR - consensus model
        DataStream<Tuple2<String, Double>> elasticPredictTrendCompareHDR = elasticCompareHDR
                .map(new MapFunction<Tuple2<String, Double>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple2<String, Double> t) throws Exception {
                        t.f0 = "PredictTrend"+t.f0;
                        return t;
                    }
                })
                .keyBy(0)
                .map( new ConsensusTrendPrediction());

        // CONSENSUS CompareXp - consensus model
        DataStream<Tuple2<String, Double>> elasticPredictTrendCompareXp = elasticCompareXp
                .map(new MapFunction<Tuple2<String, Double>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple2<String, Double> t) throws Exception {
                        t.f0 = "PredictTrend" + t.f0;
                        return t;
                    }
                })
                .keyBy(0)
                .map( new ConsensusTrendPrediction());

		//***********************
		// SINK TO ELASTICSEARCH
		//***********************
		// write all output to ElasticSearch, // ververica tutorial

		Map<String, String> config = new HashMap<>();
		config.put("bulk.flush.max.actions", "1");
		config.put("cluster.name", "elasticsearch");

		List<InetSocketAddress> transports = new ArrayList<>();
		// set default connection details
		transports.add(new InetSocketAddress(InetAddress.getByName("localhost"), 9300));

		// map the remaining metrics to elasticSearch format

		DataStream<Tuple2<String, Double>> elasticLogInOut = logInOut
				.map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Double>>() {
					@Override
					public Tuple2<String, Double> map(Tuple3<String, Integer, Integer> t) throws Exception {
						return new Tuple2<>(t.f0 +" "+ Planetside.serverIdToName(t.f1), (double)t.f2);
					}
				});

		DataStream<Tuple2<String, Double>> elasticHDR = headshotDeathRatio
				.map(new MapFunction<Tuple3<Integer, Integer, Double>, Tuple2<String, Double>>() {
					@Override
					public Tuple2<String, Double> map(Tuple3<Integer, Integer, Double> t) throws Exception {
						return new Tuple2<>("HDR", t.f2);
					}
				});

		DataStream<Tuple2<String, Double>> elasticFacility = facilityActivity
				.map(new MapFunction<Tuple4<String, String, Integer, Integer>, Tuple2<String, Double>>() {
					@Override
					public Tuple2<String, Double> map(Tuple4<String, String, Integer, Integer> t) throws Exception {
						return new Tuple2<>( t.f0 +" "+ Planetside.serverIdToName(t.f2) +" "+ t.f1, (double)t.f3 );
					}
				});

		DataStream<Tuple2<String, Double>> elasticXp = averageXpGain
				.map(new MapFunction<Tuple3<Integer, Integer, Double>, Tuple2<String, Double>>() {
					@Override
					public Tuple2<String, Double> map(Tuple3<Integer, Integer, Double> t) throws Exception {
						return new Tuple2<>("XP "+Planetside.serverIdToName(t.f0), t.f2);
					}
				});

		// add elasticSearch sinks for every metric computation

		predictedLogInOut.addSink(
				new ElasticsearchSink<>(config, transports, new BasicElasticSearchInserter()));
		predictedXpGain.addSink(
				new ElasticsearchSink<>(config, transports, new BasicElasticSearchInserter()));
		predictedHDR.addSink(
				new ElasticsearchSink<>(config, transports, new BasicElasticSearchInserter()));
		elasticLogInOut.addSink(
				new ElasticsearchSink<>(config, transports, new BasicElasticSearchInserter()));
		elasticHDR.addSink(
				new ElasticsearchSink<>(config, transports, new BasicElasticSearchInserter()));
		KDR.addSink(
				new ElasticsearchSink<>(config, transports, new PlayerKDRInserter()));
		elasticFacility.addSink(
				new ElasticsearchSink<>(config, transports, new BasicElasticSearchInserter()));
		elasticXp.addSink(
				new ElasticsearchSink<>(config, transports, new BasicElasticSearchInserter()));
        elasticCompareHDR.addSink(
                new ElasticsearchSink<>(config, transports, new BasicElasticSearchInserter()));
        elasticCompareLogInOut.addSink(
                new ElasticsearchSink<>(config, transports, new BasicElasticSearchInserter()));
        elasticCompareXp.addSink(
                new ElasticsearchSink<>(config, transports, new BasicElasticSearchInserter()));
        elasticPredictTrendCompareLogInOut.addSink(
                new ElasticsearchSink<>(config, transports, new BasicElasticSearchInserter()));
        elasticPredictTrendCompareHDR.addSink(
                new ElasticsearchSink<>(config, transports, new BasicElasticSearchInserter()));
        elasticPredictTrendCompareXp.addSink(
                new ElasticsearchSink<>(config, transports, new BasicElasticSearchInserter()));

		// execute program
        env.execute("Flink Streaming");

	}
}