package com.thecodinginterface.kinesis;

public class ShardIteratorPair {
    String shardId;
    String shardItr;

    public ShardIteratorPair(String shardId, String shardItr) {
        this.shardId = shardId;
        this.shardItr = shardItr;
    }

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    public String getShardItr() {
        return shardItr;
    }

    public void setShardItr(String shardItr) {
        this.shardItr = shardItr;
    }
}
