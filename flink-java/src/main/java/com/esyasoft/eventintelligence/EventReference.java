package com.esyasoft.eventintelligence;

public class EventReference {

    private final int eventId;
    private final int eventTblRefId;
    private final String eventName;
    private final String eventState;
    private final boolean restoration;
    private final String priorityName;
    private final String classificationName;

    public EventReference(
            int eventId,
            int eventTblRefId,
            String eventName,
            String eventState,
            boolean restoration,
            String priorityName,
            String classificationName
    ) {
        this.eventId = eventId;
        this.eventTblRefId = eventTblRefId;
        this.eventName = eventName;
        this.eventState = eventState;
        this.restoration = restoration;
        this.priorityName = priorityName;
        this.classificationName = classificationName;
    }

    public int getEventId() {
        return eventId;
    }

    public int getEventTblRefId() {
        return eventTblRefId;
    }

    public String getEventName() {
        return eventName;
    }

    public String getEventState() {
        return eventState;
    }

    public boolean isRestoration() {
        return restoration;
    }

    public String getPriorityName() {
        return priorityName;
    }

    public String getClassificationName() {
        return classificationName;
    }
}
