#!/bin/bash

# Redis configuration
REDIS_HOST="localhost"
REDIS_PORT="6379"
SOURCE_DB="1"
TARGET_DB="0"

# Clear the target database
redis-cli -h $REDIS_HOST -p $REDIS_PORT -n $TARGET_DB FLUSHDB

# Scan and copy keys from the source database to the target database
cursor=0
while true; do
    # Scan keys in the source database
    result=$(redis-cli -h $REDIS_HOST -p $REDIS_PORT -n $SOURCE_DB SCAN $cursor)
    
    # Extract cursor and keys from the result
    cursor=$(echo "$result" | head -n1)
    keys=$(echo "$result" | tail -n +2)

    # Copy each key to the target database
    for key in $keys; do
        type=$(redis-cli -h $REDIS_HOST -p $REDIS_PORT -n $SOURCE_DB TYPE $key)
        if [ "$type" = "string" ]; then
            value=$(redis-cli -h $REDIS_HOST -p $REDIS_PORT -n $SOURCE_DB GET $key)
            redis-cli -h $REDIS_HOST -p $REDIS_PORT -n $TARGET_DB SET $key "$value"
        elif [ "$type" = "list" ]; then
            values=$(redis-cli -h $REDIS_HOST -p $REDIS_PORT -n $SOURCE_DB LRANGE $key 0 -1)
            redis-cli -h $REDIS_HOST -p $REDIS_PORT -n $TARGET_DB RPUSH $key $values
        elif [ "$type" = "set" ]; then
            values=$(redis-cli -h $REDIS_HOST -p $REDIS_PORT -n $SOURCE_DB SMEMBERS $key)
            redis-cli -h $REDIS_HOST -p $REDIS_PORT -n $TARGET_DB SADD $key $values
        elif [ "$type" = "zset" ]; then
            values=$(redis-cli -h $REDIS_HOST -p $REDIS_PORT -n $SOURCE_DB ZRANGE $key 0 -1 WITHSCORES)
            redis-cli -h $REDIS_HOST -p $REDIS_PORT -n $TARGET_DB ZADD $key $values
        elif [ "$type" = "hash" ]; then
            values=$(redis-cli -h $REDIS_HOST -p $REDIS_PORT -n $SOURCE_DB HGETALL $key)
            redis-cli -h $REDIS_HOST -p $REDIS_PORT -n $TARGET_DB HMSET $key $values
        fi
    done

    # Break the loop if cursor is 0 (end of scan)
    if [ "$cursor" -eq 0 ]; then
        break
    fi
done

echo "Data copied from DB $SOURCE_DB to DB $TARGET_DB"
