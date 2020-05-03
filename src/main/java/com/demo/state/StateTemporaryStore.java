package com.demo.state;

/**
 * Created by zuihaodeziji on 2020/4/9.
 */
public class StateTemporaryStore {

   private long firstTs;
   private long currentTs;

   private int count;

   private String alarmState;

    public long getFirstTs() {
        return firstTs;
    }

    public void setFirstTs(long firstTs) {
        this.firstTs = firstTs;
    }

    public String getAlarmState() {
        return alarmState;
    }

    public void setAlarmState(String alarmState) {
        this.alarmState = alarmState;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public long getCurrentTs() {
        return currentTs;
    }

    public void setCurrentTs(long currentTs) {
        this.currentTs = currentTs;
    }
}
