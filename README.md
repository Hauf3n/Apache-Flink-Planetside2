# Apache-Flink-Planetside2-API
 Project | Stream and Batch Processing with Flink

# About
Planetside 2 is a massively multiplayer online game, in particular a first person shooter like Battlefield.<br />
It offers a (live) streaming API to collect a lot of interesting information. E.g. player login/outs, kills, base captures, vehicle destructions.<br /><br />

|| [Website](https://www.planetside2.com/home) || [API](http://census.daybreakgames.com/#what-is-websocket) || [Ingame Youtube video](https://www.youtube.com/watch?v=YfkK51dYUbI&) ||

# Tasks
1 - collect data. Use a websocket as interface <br />
2 - setup stream and batch processing <br />
3 - compare stream with batch results <br />
4 - build prediction models for some data <br />
5 - output to ElasticSearch and visualisation with Kibana<br />

# Metrics
1 - Compute the number of logins/logouts for each game server <br />
2 - Compute headshots/deaths ratio over all game servers <br />
3 - Compute kills/deaths ratio for every active player <br />
4 - Compute the region with max player activity for each game server <br />
5 - Compute the average experience gain for each game server <br />

# Predicion 
1 - Online iterative mean model <br />
2 - Online consensus model

# Kibana Visualization | ElasticSearch
![kibana](https://github.com/Hauf3n/Apache-Flink-Planetside2-API/blob/master/media/kibana_dashboard.png)
