package com.esyasoft.eventintelligence;

import java.io.Serializable;

public class OutageState implements Serializable {

    private String msn;
    private int eventTblRefId;
    private String eventName;

    private String startTime;

    private String dtrName;
    private String dtrNetworkCode;
    private String feederName;
    private String substationName;

    /*
     * Processing-time timer used to detect an outage
     * that remains unresolved for more than 5 minutes.
     */
    private Long alertTimerTimestamp;

    public OutageState() {
        // Required for Flink POJO serialization
    }

    /*
     * Original constructor retained for compatibility.
     */
    public OutageState(
            String msn,
            int eventTblRefId,
            String eventName,
            String startTime,
            String dtrName,
            String dtrNetworkCode,
            String feederName,
            String substationName
    ) {
        this(
                msn,
                eventTblRefId,
                eventName,
                startTime,
                dtrName,
                dtrNetworkCode,
                feederName,
                substationName,
                null
        );
    }

    /*
     * Constructor including unresolved-outage timer.
     */
    public OutageState(
            String msn,
            int eventTblRefId,
            String eventName,
            String startTime,
            String dtrName,
            String dtrNetworkCode,
            String feederName,
            String substationName,
            Long alertTimerTimestamp
    ) {
        this.msn = msn;
        this.eventTblRefId = eventTblRefId;
        this.eventName = eventName;
        this.startTime = startTime;
        this.dtrName = dtrName;
        this.dtrNetworkCode = dtrNetworkCode;
        this.feederName = feederName;
        this.substationName = substationName;
        this.alertTimerTimestamp = alertTimerTimestamp;
    }

    public String getMsn() {
        return msn;
    }

    public void setMsn(String msn) {
        this.msn = msn;
    }

    public int getEventTblRefId() {
        return eventTblRefId;
    }

    public void setEventTblRefId(int eventTblRefId) {
        this.eventTblRefId = eventTblRefId;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getDtrName() {
        return dtrName;
    }

    public void setDtrName(String dtrName) {
        this.dtrName = dtrName;
    }

    public String getDtrNetworkCode() {
        return dtrNetworkCode;
    }

    public void setDtrNetworkCode(String dtrNetworkCode) {
        this.dtrNetworkCode = dtrNetworkCode;
    }

    public String getFeederName() {
        return feederName;
    }

    public void setFeederName(String feederName) {
        this.feederName = feederName;
    }

    public String getSubstationName() {
        return substationName;
    }

    public void setSubstationName(String substationName) {
        this.substationName = substationName;
    }

    public Long getAlertTimerTimestamp() {
        return alertTimerTimestamp;
    }

    public void setAlertTimerTimestamp(
            Long alertTimerTimestamp
    ) {
        this.alertTimerTimestamp =
                alertTimerTimestamp;
    }
}
