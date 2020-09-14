package org.myorg.project;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.project.filterStreaming.*;
import org.myorg.project.mapStreaming.StreamMapEventJsonPayload;

/*
    DataCollection - Main class for data collection
*/

public class DataCollectionOffline {

    public static void main(String[] args) throws Exception {

        // collect data for offline processing
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // connect to websocket interface
        DataStream<String> rawEvents = env.socketTextStream("localhost",1337);
        DataStream<String> rawPayloadEvents = rawEvents.filter( new StreamFilterEventPayload());
        DataStream<JsonNode> jsonPayloadEvents = rawPayloadEvents.map( new StreamMapEventJsonPayload());

        // save data
        DataStreamSink<JsonNode> jsonLogInOutEvents = jsonPayloadEvents.filter( new StreamFilterEventLogInOut())
                .writeAsText("offline_data/LogInOut");

        DataStreamSink<JsonNode> jsonDeathEvents = jsonPayloadEvents.filter( new StreamFilterEventDeath())
                .writeAsText("offline_data/Death");

        DataStreamSink<JsonNode> jsonFacilityEvents = jsonPayloadEvents.filter( new StreamFilterEventFacility())
                .writeAsText("offline_data/Facility");

        DataStreamSink<JsonNode> jsonExperienceEvents = jsonPayloadEvents.filter( new StreamFilterEventExperience())
                .writeAsText("offline_data/Experience");

        // execute program
        env.execute("collect offline data");
    }
}
