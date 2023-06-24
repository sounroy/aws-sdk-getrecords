package com.thecodinginterface.kinesis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class GetRecordsApp {
    static final Logger logger = LogManager.getLogger(GetRecordsApp.class);
    static final ObjectMapper objMapper = new ObjectMapper();

    public static void main(String[] args) {
        logger.info("Starting GetRecords Consumer");
        String streamName = args[0];

        // Instantiate the client
        var client = KinesisClient.builder().build();

        // Add shutdown hook to cleanly close the client when program terminates
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down program");
            client.close();
        }, "consumer-shutdown"));


        // Collect list of Shard Id and Shard Iterator Pairs for Fetching Records Per Shard
        logger.info("Fetching Shards and Iterators");
        var describeStreamReq = DescribeStreamRequest.builder().streamName(streamName).build();

        List<ShardIteratorPair> shardIterators = new ArrayList<>();
        DescribeStreamResponse describeStreamRes;

        do {
            describeStreamRes = client.describeStream(describeStreamReq);
            for (Shard shard : describeStreamRes.streamDescription().shards()) {
                // start from last untrimmed record in the shard in the system
                // (the oldest data record in the shard)
                var itrRequest = GetShardIteratorRequest.builder()
                                                .streamName(streamName)
                                                .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                                                .shardId(shard.shardId())
                                                .build();
                var shardItrRes = client.getShardIterator(itrRequest);
                var shardItr = shardItrRes.shardIterator();

                shardIterators.add(new ShardIteratorPair(shard.shardId(), shardItr));
            }
        } while(describeStreamRes.streamDescription().hasMoreShards());

        // Continuously Poll the Shards, collecting Records and updating to use the latest Shard Iterators
        while (true) {
            for (var shardItrPair : shardIterators) {

                // construct request to GetRecords up to a limit using the given Shard Iterator
                var recordsReq = GetRecordsRequest.builder()
                        .shardIterator(shardItrPair.getShardItr())
                        .limit(200)
                        .build();

                // execute GetRecords API call
                GetRecordsResponse recordsRes = client.getRecords(recordsReq);
                shardItrPair.setShardItr(recordsRes.nextShardIterator());

                if (recordsRes.hasRecords()) {
                    // iterate over the fetched records, parsing and deserializing to Order objects
                    for (var record : recordsRes.records()) {
                        var orderStr = new String(record.data().asByteArray(), StandardCharsets.UTF_8);
                        try {
                            var order = objMapper.readValue(orderStr, Order.class);
                            logger.info(String.format("Read Order %s from shard %s at position %s",
                                    order,
                                    shardItrPair.getShardId(),
                                    record.sequenceNumber()));
                        } catch (JsonProcessingException e) {
                            logger.error("Failed to deserialize record", e);
                        }
                    }
                }
            }

            // introduce artificial pause to allow for log viewing during demo
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                logger.error("Sleep Interrupted", e);
            }
        }
    }
}
