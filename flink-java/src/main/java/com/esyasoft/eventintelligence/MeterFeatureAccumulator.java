package com.esyasoft.eventintelligence;

import java.io.Serializable;

public class MeterFeatureAccumulator
        implements Serializable {

    public long eventCount;

    public long voltageCount;
    public double voltageSum;

    public Double minVoltage;
    public Double maxVoltage;

    public long powerFailureCount;

    public MeterFeatureAccumulator() {
        // Required for Flink serialization
    }

    public void addVoltage(double voltage) {

        voltageSum += voltage;
        voltageCount++;

        if (minVoltage == null
                || voltage < minVoltage) {
            minVoltage = voltage;
        }

        if (maxVoltage == null
                || voltage > maxVoltage) {
            maxVoltage = voltage;
        }
    }
}
