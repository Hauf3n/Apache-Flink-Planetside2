package org.myorg.project;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.myorg.project.mapBatch.*;
import org.myorg.project.mapStreaming.StreamFlatmapEventDeathKDR;
import org.myorg.project.mapStreaming.StreamMapEventDeathKDR;
import org.myorg.project.mapStreaming.StreamMapEventHeadshot;
import org.myorg.project.reduceBatch.BatchReduceEventExperienceCountPlayerAndXpPerInterval;
import org.myorg.project.reduceBatch.BatchReduceEventExperienceSumAllAvgXpAndIntervals;
import org.myorg.project.reduceBatch.BatchReduceGroupAvgLogInOut;
import org.myorg.project.reduceBatch.BatchReduceGroupHeadshotRatio;
import org.myorg.project.reduceStream.StreamReduceEventDeathKDR;

/*
    BatchJob - Main class for the batch computations
    Content:
    - SETUP
    - Compute 5 METRICS
    - SINK TO STANDARD FILE
*/

public class BatchJob {

	public static void main(String[] args) throws Exception {

		//******************
		// SETUP
		//******************

		// group the offline data (sometimes) by its timestamps into time intervals in order to compare it with the streaming metrics
		int groupIntervalSeconds = 30;

		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//******************
		// Compute 5 METRICS
		//******************

		//******************
		//        1
		//******************
		// Compute the avg number of logins/logouts for each world for a given time interval
		// format: event,world_id,avg count
		// goal: track the logInOut traffic
		DataSet<String> rawLogInOut = env.readTextFile("offline_data/LogInOut");
		DataSet<JsonNode> jsonLogInOut = rawLogInOut.map( new BatchMapEventStringToJson());
		DataSet<JsonNode> jsonTimeIntervalLogInOut = jsonLogInOut.map( new BatchMapTimestampToInterval(groupIntervalSeconds));

		DataSet<Tuple3< String, Integer, Double>> logInOut = jsonTimeIntervalLogInOut
				.map( new BatchMapEventLogInOut())
				.groupBy(0,1,2)
				.sum(3)
				.groupBy(0,2)
				.reduceGroup(new BatchReduceGroupAvgLogInOut());

		//******************
		//        2
		//******************
        // Compute headshots/deaths ratio over all worlds
        // format: num headshots, num deaths, ratio
        // goal: track player headshot accuracy
        DataSet<String> rawDeath = env.readTextFile("offline_data/Death");
        DataSet<JsonNode> jsonDeath = rawDeath.map( new BatchMapEventStringToJson());

        DataSet<Tuple3<Integer, Integer, Double>> headShotDeathRatio = jsonDeath
                .map( new StreamMapEventHeadshot())
                .groupBy(0)
                .reduceGroup( new BatchReduceGroupHeadshotRatio());

		//******************
		//        3
		//******************
        // Compute kills/deaths ratio for every player
        // format: character, kd ratio
        // goal: track player skill
        DataSet<Tuple2<String, Double>> KDR = jsonDeath
                .flatMap( new StreamFlatmapEventDeathKDR())
                .groupBy(0)
                .reduce( new StreamReduceEventDeathKDR())
                .map( new StreamMapEventDeathKDR());

		//******************
		//        4
		//******************
        // Compute the facility with max activity for each world
        // format: event, facility id, world id, timestamp, number of participating players
        // goal: find the region which has the largest fight
        DataSet<String> rawFacility = env.readTextFile("offline_data/Facility");
        DataSet<JsonNode> jsonFacility = rawFacility.map( new BatchMapEventStringToJson());

        DataSet<Tuple5<String, String, Integer, String, Integer>> facilityActivity = jsonFacility
                .map( new BatchMapEventFacility())
                .groupBy(0,1,2,3)
                .sum(4)
                .groupBy(2)
                .maxBy(4);

		//******************
		//        5
		//******************
        // Compute the average experience gain per player for each world, average over all time intervals
        // format: world id, average xp gain per time interval (averaged over all time intervals)
        // goal: get a baseline for avg xp gain and overall avg player performance
        DataSet<String> rawXpGain = env.readTextFile("offline_data/Experience");
        DataSet<JsonNode> jsonXpGain = rawXpGain.map( new BatchMapEventStringToJson());
        DataSet<JsonNode> jsonTimeIntervalXpGain = jsonXpGain.map( new BatchMapTimestampToInterval(groupIntervalSeconds));

        DataSet<Tuple2<String, Double>> averageXpGain = jsonTimeIntervalXpGain
                .map( new BatchMapEventExperience())
                .groupBy(0,1,2)
                .sum(3)
                .map(new BatchMapEventExperienceSetupPlayerCountPerInterval())
                .groupBy(0,2)
                .reduce(new BatchReduceEventExperienceCountPlayerAndXpPerInterval())
                .map(new BatchMapEventExperienceAvgXpPerInterval())
                .groupBy(0)
                .reduce(new BatchReduceEventExperienceSumAllAvgXpAndIntervals())
                .map(new BatchMapEventExperienceXpAvgOfAllXpAvgIntervals());

		//**********************
		// SINK TO STANDARD FILE
		//**********************
		// write output to file, the files will be read by the streaming job

		logInOut.writeAsCsv("batch_processing_result/LogInOut");
		headShotDeathRatio.writeAsCsv("batch_processing_result/HDR");
		KDR.writeAsCsv("batch_processing_result/KDR");
		facilityActivity.writeAsCsv("batch_processing_result/FacilityActivity");
		averageXpGain.writeAsCsv("batch_processing_result/XP");

		// execute program
		env.execute("Flink Batch");
	}
}
