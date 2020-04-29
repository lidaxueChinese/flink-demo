package com.demo.cep;
/**
 * Created by lidaxue on 2020/4/28.
 */
public class Event {

    public Event(String id,String name){
        this.id = id;
        this.name = name;
    }

    private String id;

    private String name;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return getId()+","+getName()+"|";
    }
}
