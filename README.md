# SparkStructuredStreaming
Spark Structured Streaming is a powerful tool for handling streaming data. Spark Structured Streaming and the previous Spark Streaming established the framework for micro-batching the incoming streaming data. I have written a few articles in Linkedlin describing how a typical streaming for a trading system is designed and how it works. For an  example see the following article

The concept of streaming normally alludes to a form of information sent as messages (trade data, IOT, similar) in a regular, near real time and always on so to speak. However, that is not the whole picture. You may have a situation where you send data on a regular basic. For example extracting data from upstream sources through some CDC tool like Oracle Golden Gate or anything that writes to files.

The extraction process (with whatever tool) is not the focus of this article. We are interested in the by-product which are normally referred to as trail files. These file contain changed data capture information. For example, Golden Gate extracts committed transactions from the transaction logs (the redo logs) of RDBMS. These extraction process creates trail files. These trail files land in a persistent directory, This directory can be setup accessible both to on-premise and Cloud storage (see later).

