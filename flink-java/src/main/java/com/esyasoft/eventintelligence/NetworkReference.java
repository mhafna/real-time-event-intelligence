package com.esyasoft.eventintelligence;

public class NetworkReference {

    private final String substationName;
    private final String feederName;
    private final String dtrName;
    private final String dtrNetworkCode;
    private final String msn;

    public NetworkReference(
            String substationName,
            String feederName,
            String dtrName,
            String dtrNetworkCode,
            String msn
    ) {
        this.substationName = substationName;
        this.feederName = feederName;
        this.dtrName = dtrName;
        this.dtrNetworkCode = dtrNetworkCode;
        this.msn = msn;
    }

    public String getSubstationName() {
        return substationName;
    }

    public String getFeederName() {
        return feederName;
    }

    public String getDtrName() {
        return dtrName;
    }

    public String getDtrNetworkCode() {
        return dtrNetworkCode;
    }

    public String getMsn() {
        return msn;
    }
}
