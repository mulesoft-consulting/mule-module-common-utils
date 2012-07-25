package com.mulesoft.module.utils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mule.api.registry.MuleRegistry;
import org.mule.api.registry.Registry;

import com.hazelcast.core.HazelcastInstance;
import com.mulesoft.mule.cluster.hazelcast.HazelcastManager;

public class ThrottleQueueManager {

	protected static final Log logger = LogFactory.getLog(ThrottleQueueManager.class);
	
	private Registry registry;
	
	public ThrottleQueueManager(Registry registry) {
		this.registry = registry;
	}
	
	public BlockingQueue getQueue(String key, int capacity) throws Exception {
		
		BlockingQueue queue = null;
		
		if (HazelcastManager.hasInstance()) {
			logger.info("Mule clustering is enabled; using Hazelcast queue");
			HazelcastInstance instance = HazelcastManager.getInstance().getHazelcastInstance();
			queue = instance.getQueue(key);
		} else {
			logger.info("Mule clustering is disabled; using local queue");
			queue = registry.lookupObject(key);
			if (queue == null) {
				queue = new ArrayBlockingQueue<Long>(capacity);
				registry.registerObject(key, queue);
			}
		}
		
		return queue;
	}
}
