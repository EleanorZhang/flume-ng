/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.instrumentation;

public class ChannelCounter extends MonitoredCounterGroup implements
    ChannelCounterMBean {

  private static final String COUNTER_CHANNEL_SIZE = "channel.current.size";

  private static final String COUNTER_EVENT_PUT_ATTEMPT =
      "channel.event.put.attempt";

  private static final String COUNTER_EVENT_TAKE_ATTEMPT =
      "channel.event.take.attempt";

  private static final String COUNTER_EVENT_PUT_SUCCESS =
      "channel.event.put.success";

  private static final String COUNTER_EVENT_TAKE_SUCCESS =
      "channel.event.take.success";

  private static final String COUNTER_CHANNEL_CAPACITY =
          "channel.capacity";

  private static final String COUNTER_CHANNEL_INPUT_COUNT =
      "channel.inputCount";

  private static final String COUNTER_CHANNEL_OUTPUT_COUNT =
      "channel.outputCount";

  private static final String COUNTER_CHANNEL_LAST_RESET =
      "channel.lastReset";

  private static final String COUNTER_CHANNEL_FIRST_RESET =
      "channel.firstReset";

  private static final String[] ATTRIBUTES = {
    COUNTER_CHANNEL_SIZE, COUNTER_EVENT_PUT_ATTEMPT,
    COUNTER_EVENT_TAKE_ATTEMPT, COUNTER_EVENT_PUT_SUCCESS,
    COUNTER_EVENT_TAKE_SUCCESS, COUNTER_CHANNEL_CAPACITY,
    COUNTER_CHANNEL_INPUT_COUNT, COUNTER_CHANNEL_OUTPUT_COUNT,
    COUNTER_CHANNEL_LAST_RESET, COUNTER_CHANNEL_FIRST_RESET
  };

  public ChannelCounter(String name) {
    super(MonitoredCounterGroup.Type.CHANNEL, name, ATTRIBUTES);
  }

  @Override
  public long getChannelSize() {
    return get(COUNTER_CHANNEL_SIZE);
  }

  public void setChannelSize(long newSize) {
    set(COUNTER_CHANNEL_SIZE, newSize);
  }

  @Override
  public long getEventPutAttemptCount() {
    return get(COUNTER_EVENT_PUT_ATTEMPT);
  }

  public long incrementEventPutAttemptCount() {
    return increment(COUNTER_EVENT_PUT_ATTEMPT);
  }

  @Override
  public long getEventTakeAttemptCount() {
    return get(COUNTER_EVENT_TAKE_ATTEMPT);
  }

  public long incrementEventTakeAttemptCount() {
    return increment(COUNTER_EVENT_TAKE_ATTEMPT);
  }

  @Override
  public long getEventPutSuccessCount() {
    return get(COUNTER_EVENT_PUT_SUCCESS);
  }

  public long addToEventPutSuccessCount(long delta) {
      addAndGet(COUNTER_CHANNEL_INPUT_COUNT, delta);
    return addAndGet(COUNTER_EVENT_PUT_SUCCESS, delta);
  }

  @Override
  public long getEventTakeSuccessCount() {
    return get(COUNTER_EVENT_TAKE_SUCCESS);
  }

  public long addToEventTakeSuccessCount(long delta) {
      addAndGet(COUNTER_CHANNEL_OUTPUT_COUNT, delta);
    return addAndGet(COUNTER_EVENT_TAKE_SUCCESS, delta);
  }

  public void setChannelCapacity(long capacity){
    set(COUNTER_CHANNEL_CAPACITY, capacity);
  }

  @Override
  public long getChannelCapacity(){
    return get(COUNTER_CHANNEL_CAPACITY);
  }

  @Override
  public double getChannelFillPercentage(){
    long capacity = getChannelCapacity();
    if(capacity != 0L) {
	return (double) (int)((getChannelSize()/(double)capacity) * 10000) / 100;
    }
    return Double.MAX_VALUE;
  }

  public long getInputCount(){
    return get(COUNTER_CHANNEL_INPUT_COUNT);
  }

  public long getOutputCount(){
    return get(COUNTER_CHANNEL_OUTPUT_COUNT);
  }

    public long getLastReset(){
	return get(COUNTER_CHANNEL_LAST_RESET);
    }

    public void setLastReset(long currentTime){
	set(COUNTER_CHANNEL_LAST_RESET, currentTime);
    }

    public long getFirstReset(){
	return get(COUNTER_CHANNEL_FIRST_RESET);
    }
    
    // Fully reset metrics
    public void resetStats(long currentTime){
	set(COUNTER_CHANNEL_INPUT_COUNT, 0L);
	set(COUNTER_CHANNEL_OUTPUT_COUNT, 0L);
	set(COUNTER_CHANNEL_FIRST_RESET, currentTime);
    }

    // Returns input rate per second
    public double getInputRate(long currentTime)
    {
	long diff = currentTime - getFirstReset();
	return (double) (int) (getInputCount() * 100000 / (double) diff) / 100;
    }

    // Returns output rate per second
    public double getOutputRate(long currentTime)
    {
	long diff = currentTime - getFirstReset();
	return (double) (int) (getOutputCount() * 100000 / (double) diff) / 100;
    }

    // Returns the time in ms until the channel is overloaded
    public int getOverloadEstimate(long currentTime)
    {
	double inputRate = getInputRate(currentTime);
	double outputRate = getOutputRate(currentTime);
	double diff = inputRate - outputRate;
	// Not overloaded, return a large value for estimated time
	if(diff<=0.0)
	    return Integer.MAX_VALUE;
	// Remaining capacity
	double rem = (double) (getChannelCapacity() - getChannelSize());
	// Diff is in seconds, convert to ms
	return (int) (rem * 1000 / diff);	
    }

    // Uses the M/M/1/K queueing theory equation
    public double getWaitTime(long currentTime)
    {
        double inputRate = getInputRate(currentTime);
        double outputRate = getOutputRate(currentTime);
	// Traffic intensity
        double a = inputRate / outputRate;
	double K = (double) getChannelCapacity();
	double L = a / (1 - a) - (((K+1) * Math.pow(a, K+1))/ (1 - Math.pow(a, K+1)));
	// input rate is in seconds, convert to ms
	double res = L * 1000 / inputRate;
	// Usually due to a divide by 0 computation, not overloaded
	if(Double.isNaN(res))
	    res = 0.0;
	return res;
    }

    // Traffic intensity, unused right now
    public double getLoad(long currentTime)
    {
	double inputRate = getInputRate(currentTime);
	double outputRate = getOutputRate(currentTime);
	return outputRate == 0.0 ? outputRate : inputRate / outputRate;
    }

}
