package com.intrence.datapipeline.tailor.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import com.intrence.datapipeline.tailor.net.FetchRequest;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Tuple;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;

public class RedisFetchRequestQueue implements FetchRequestQueue {

    private static final Logger LOGGER = Logger.getLogger(RedisFetchRequestQueue.class);
    private static final String CACHE_PREFIX = "cache-";

    private JedisPool jedisPool;
    private Set<HashFunction> hashFunctions;

    @Inject
    public RedisFetchRequestQueue(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @PostConstruct
    public void init() {
        this.hashFunctions = new HashSet<>(Arrays.asList(Hashing.adler32(), Hashing.crc32(), Hashing.murmur3_32(), Hashing.crc32c()));
    }

    @PreDestroy
    public void shutdown() {
        this.jedisPool.destroy();
    }

    @Override
    public FetchRequest getNext(Integer taskRuleId) {

        Jedis jedis = null;
        FetchRequest fetchRequest = null;
        long start = System.currentTimeMillis();

        try {
            jedis = this.jedisPool.getResource();
            Set<Tuple> set = jedis.zrevrangeWithScores(String.valueOf(taskRuleId), 0, 0);
            for (Tuple tuple : set) {
                fetchRequest = FetchRequest.fromJson(tuple.getElement());
                break;
            }
            return fetchRequest;
        } finally {
            close(jedis);
            logTime("RedisGetNext", taskRuleId, start);
        }
    }

    @Override
    public void delete(Integer taskRuleId, FetchRequest fetchRequest) {
        Jedis jedis = null;
        long start = System.currentTimeMillis();
        try {
            jedis = jedisPool.getResource();
            jedis.zrem(String.valueOf(taskRuleId), fetchRequest.toJson());
        } catch (JsonProcessingException e) {
            //json processing exception
            LOGGER.warn(String.format("Exception=%s when deleting fetchRequest=%s to redis for taskrule=%s",
                                      e.getMessage(),
                                      fetchRequest.getWorkRequest(),
                                      taskRuleId));
        } finally {
            close(jedis);
            logTime("RedisDelete", taskRuleId, start);
        }
    }

    @Override
    public Set<FetchRequest> getTopK(Integer taskRuleId, Integer K) {
        Set<FetchRequest> topK = new HashSet<>();
        Jedis jedis = null;
        long start = System.currentTimeMillis();
        try {
            jedis = jedisPool.getResource();
            LOGGER.info("jedis connection " + jedis.info());
            if (K > 0) {
                Set<Tuple> set = jedis.zrevrangeWithScores(String.valueOf(taskRuleId), new Long(0), new Long(K - 1));
                for (Tuple tuple : set) {
                    topK.add(FetchRequest.fromJson(tuple.getElement()));
                }
            }
            return topK;
        } finally {
            close(jedis);
            logTime("RedisGetTopK", taskRuleId, start);
        }
    }

    @Override
    public void add(Integer taskRuleId, FetchRequest fetchRequest) {
        Jedis jedis = null;
        long start = System.currentTimeMillis();
        String fetchRequestJson;

        try {
            jedis = this.jedisPool.getResource();
            if (!isSeen(jedis, taskRuleId, fetchRequest)) {
                fetchRequestJson = fetchRequest.toJson();
                jedis.zadd(String.valueOf(taskRuleId), fetchRequest.getPriority(), fetchRequestJson);
                addToCache(jedis, taskRuleId, fetchRequestJson);
            }
        } catch (JsonProcessingException e) {
            //json processing exception
            LOGGER.warn(String.format("Exception=%s when adding fetchRequest=%s to redis for taskrule=%s",
                                      e.getMessage(),
                                      fetchRequest.getWorkRequest(),
                                      taskRuleId));
        } finally {
            close(jedis);
            logTime("RedisAdd", taskRuleId, start);
        }
    }

    // redis has limits on number of entries that could be added to set in single shot
    //  - so, adding in batches
    @Override
    public void addAll(Integer taskRuleId, Set<FetchRequest> fetchReqs) {
        int maxInABatch = 1000;
        int count = 0;
        Set<FetchRequest> curBatch = new HashSet<>();
        for (FetchRequest req : fetchReqs) {
            if (req != null) {
                curBatch.add(req);
                count++;
                if (count >= maxInABatch) {
                    addAllInternal(taskRuleId, curBatch);
                    count = 0;
                    curBatch = new HashSet<>();
                }
            }
        }
        if (!curBatch.isEmpty()) {
            addAllInternal(taskRuleId, curBatch);
        }
    }

    @Override
    public void deleteAll(Integer taskRuleId, Set<FetchRequest> fetchRequests) {

        if (CollectionUtils.isEmpty(fetchRequests)) {
            return;
        }

        Jedis jedis = null;
        long start = System.currentTimeMillis();
        try {
            jedis = jedisPool.getResource();
            String[] members = new String[fetchRequests.size()];
            int i = 0;
            for (FetchRequest fetchReq : fetchRequests) {
                members[i] = fetchReq.toJson();
                i++;
            }
            jedis.zrem(String.valueOf(taskRuleId), members);
        } catch (JsonProcessingException e) {
            //json processing exception
            LOGGER.warn(String.format(
                    "Exception=%s when deleting set of fetchRequests(size=%s) to redis for taskrule=%s",
                    e.getMessage(),
                    fetchRequests.size(),
                    taskRuleId));
        } finally {
            close(jedis);
            logTime("RedisDeleteAll", taskRuleId, start);
        }
    }

    @Override
    public void deleteKey(Integer taskRuleId) {
        Jedis jedis = null;
        long start = System.currentTimeMillis();
        try {
            jedis = jedisPool.getResource();
            jedis.del(String.valueOf(taskRuleId), CACHE_PREFIX + taskRuleId);
        }
        finally {
            close(jedis);
            logTime("RedisDeleteKey", taskRuleId, start);
        }
    }

    // currently only used in test
    public void addToCache(Integer taskRuleId, String value) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            addToCache(jedis, taskRuleId, value);
        }
        finally {
            close(jedis);
        }
    }

    public boolean isSeen(Integer taskRuleId, FetchRequest fetchReq) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return isSeen(jedis, taskRuleId, fetchReq);
        }
        catch (JsonProcessingException e) {
            //json processing exception
            LOGGER.warn(String.format("Exception=%s when checking isSeen for fetchRequest=%s to redis for taskrule=%s",
                                      e.getMessage(),
                                      fetchReq.getWorkRequest(),
                                      taskRuleId));
            return false;
        }
        finally {
            close(jedis);
        }
    }

    protected Set<Long> hash(String s) {
        Set<Long> hashes = new HashSet<>();
        for (HashFunction hashFn : this.hashFunctions) {
            HashCode hashCode = hashFn.hashUnencodedChars(s);
            Integer h = hashCode.asInt();
            if (h < 0) {
                Integer positiveH = Math.abs(h);
                hashes.add((long) Integer.MAX_VALUE + (long) positiveH);
            } else {
                hashes.add((long) h);
            }
        }
        return hashes;
    }

    protected boolean isSeen(Jedis jedis, Integer taskRuleId, FetchRequest fetchReq) throws JsonProcessingException {
        return _isSeen(jedis, (CACHE_PREFIX + taskRuleId), fetchReq.toJson());
    }

    protected boolean _isSeen(Jedis jedis, String key, String value) {
        Set<Long> hashes = hash(value);
        boolean seen = true;
        for (long hash : hashes) {
            seen = seen & jedis.getbit(key, hash);
        }
        return seen;
    }

    protected void close(Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }

    protected void logTime(String event, Integer key, long start) {
        LOGGER.info(String.format("Event=%s key=%s timeTaken=%s", event, key, (System.currentTimeMillis() - start)));
    }

    protected void logTime(String event, String key, long start) {
        LOGGER.info(String.format("Event=%s key=%s timeTaken=%s", event, key, (System.currentTimeMillis() - start)));
    }

    protected void addToCache(Jedis jedis, Integer taskRuleId, String value) {
        _addToCache(jedis, (CACHE_PREFIX + taskRuleId), value);
    }

    protected void _addToCache(Jedis jedis, String key, String value) {
        Set<Long> hashes = hash(value);
        for (long hash : hashes) {
            jedis.setbit(key, hash, true);
        }
    }

    private void addAllInternal(Integer taskRuleId, Set<FetchRequest> fetchReqs) {
        Jedis jedis = null;
        long start = System.currentTimeMillis();
        try {
            jedis = jedisPool.getResource();

            Map<String, Double> scoredMembers = new HashMap<>();
            for (FetchRequest fetchReq : fetchReqs) {
                if (!isSeen(jedis, taskRuleId, fetchReq)) {
                    scoredMembers.put(fetchReq.toJson(), (double) fetchReq.getPriority());
                }
            }
            if (!scoredMembers.isEmpty()) {
                jedis.zadd(String.valueOf(taskRuleId), scoredMembers);
                for (String member : scoredMembers.keySet()) {
                    addToCache(jedis, taskRuleId, member);
                }
            }
        } catch (JsonProcessingException e) {
            //json processing exception
            LOGGER.warn(String.format("Exception=%s when adding set of fetchRequests(size=%s) to redis for taskrule=%s",
                                      e.getMessage(),
                                      fetchReqs.size(),
                                      taskRuleId));
        } finally {
            close(jedis);
            logTime("RedisAddAll", taskRuleId, start);
        }
    }
}
