package com.esyasoft.eventintelligence;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class UseCasePluginLoader {

    private final ObjectMapper objectMapper =
            new ObjectMapper();

    public List<LoadedPlugin> loadEnabledPlugins() {

        try {

            InputStream inputStream =
                    UseCasePluginLoader.class
                            .getClassLoader()
                            .getResourceAsStream(
                                    "usecases.json"
                            );

            if (inputStream == null) {
                throw new IllegalStateException(
                        "usecases.json was not found"
                );
            }

            UseCaseConfiguration configuration =
                    objectMapper.readValue(
                            inputStream,
                            UseCaseConfiguration.class
                    );

            List<LoadedPlugin> loadedPlugins =
                    new ArrayList<>();

            for (UseCaseDefinition definition
                    : configuration.getPlugins()) {

                if (!definition.isEnabled()) {
                    continue;
                }

                Class<?> pluginClass =
                        Class.forName(
                                definition.getClassName()
                        );

                if (!UseCasePlugin.class
                        .isAssignableFrom(pluginClass)) {

                    throw new IllegalArgumentException(
                            definition.getClassName()
                                    + " does not implement "
                                    + "UseCasePlugin"
                    );
                }

                UseCasePlugin plugin =
                        (UseCasePlugin)
                                pluginClass
                                        .getDeclaredConstructor()
                                        .newInstance();

                loadedPlugins.add(
                        new LoadedPlugin(
                                definition,
                                plugin
                        )
                );
            }

            return loadedPlugins;

        } catch (Exception e) {

            throw new RuntimeException(
                    "Unable to load use-case plugins",
                    e
            );
        }
    }

    public static class LoadedPlugin {

        private final UseCaseDefinition definition;
        private final UseCasePlugin plugin;

        public LoadedPlugin(
                UseCaseDefinition definition,
                UseCasePlugin plugin
        ) {
            this.definition = definition;
            this.plugin = plugin;
        }

        public UseCaseDefinition getDefinition() {
            return definition;
        }

        public UseCasePlugin getPlugin() {
            return plugin;
        }
    }
}
