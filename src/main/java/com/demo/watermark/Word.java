package com.demo.watermark;

/**
 * Created by zuihaodeziji on 2020/4/17.
 */
public class Word {

    private String name;

    private Long eventTime;

    private Integer count;

    @Override
    public String toString() {
        return "name is:"+getName()+",eventTime is:"+getEventTime()+",count is:"+getCount();
    }

    public Word(){

    }

    public Word(String name, Long eventTime, Integer count){
        this.name = name;
        this.eventTime = eventTime;
        this.count = count;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
}
