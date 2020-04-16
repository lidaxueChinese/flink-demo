package com.demo.state;

/**
 * Created by zuihaodeziji on 2020/4/9.
 */
public class StateTemporaryStore {

   private long firstTs;

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
}
