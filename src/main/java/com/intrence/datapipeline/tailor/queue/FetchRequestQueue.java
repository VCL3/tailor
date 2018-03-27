package com.intrence.datapipeline.tailor.queue;

import com.intrence.datapipeline.tailor.net.FetchRequest;

import java.util.Set;

public interface FetchRequestQueue {

    FetchRequest getNext(Integer taskRuleId);
    
    Set<FetchRequest> getTopK(Integer taskRuleId, Integer K);
    
    void add(Integer taskRuleId, FetchRequest fetchRequest);
    
    void addAll(Integer taskRuleId, Set<FetchRequest> fetchRequests);
    
    void delete(Integer taskRuleId, FetchRequest fetchRequest);

    void deleteAll(Integer taskRuleId, Set<FetchRequest> fetchRequests);

    // deletes the taskRuleId key and all its associated values
    void deleteKey(Integer taskRuleId);
}
