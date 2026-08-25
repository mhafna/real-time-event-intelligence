package com.esyasoft.eventintelligence;

import java.util.ArrayList;
import java.util.List;

public class UseCaseConfiguration {

    private List<UseCaseDefinition> plugins =
            new ArrayList<>();

    public UseCaseConfiguration() {
    }

    public List<UseCaseDefinition> getPlugins() {
        return plugins;
    }

    public void setPlugins(
            List<UseCaseDefinition> plugins
    ) {
        this.plugins = plugins;
    }
}
