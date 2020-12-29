package com.iflytek;

public class LogEvent {

    private String userId;
    private String userName;
    private String eventId;
    private String eventTime;
    private String area;
    private String ip;
    private String mapAddress;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getMapAddress() {
        return mapAddress;
    }

    public void setMapAddress(String mapAddress) {
        this.mapAddress = mapAddress;
    }

    @Override
    public String toString() {
        return "LogEvent{" +
                "userId='" + userId + '\'' +
                ", userName='" + userName + '\'' +
                ", eventId='" + eventId + '\'' +
                ", eventTime='" + eventTime + '\'' +
                ", area='" + area + '\'' +
                ", ip='" + ip + '\'' +
                ", mapAddress='" + mapAddress + '\'' +
                '}';
    }
}
