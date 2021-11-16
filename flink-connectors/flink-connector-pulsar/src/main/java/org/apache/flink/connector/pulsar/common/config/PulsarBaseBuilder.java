package org.apache.flink.connector.pulsar.common.config;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.sink.PulsarSinkOptions;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SERVICE_URL;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public abstract class PulsarBaseBuilder<T extends PulsarBaseBuilder> {
    protected final Configuration configuration;

    public PulsarBaseBuilder() {
        this.configuration = new Configuration();
    }

    /**
     * Sets the admin endpoint for the PulsarAdmin of the PulsarSink.
     *
     * @param adminUrl the url for the PulsarAdmin.
     * @return this PulsarSinkBuilder.
     */
    public T setAdminUrl(String adminUrl) {
        return setConfig(PULSAR_ADMIN_URL, adminUrl);
    }

    /**
     * Sets the server's link for the PulsarProducer of the PulsarSink.
     *
     * @param serviceUrl the server url of the Pulsar cluster.
     * @return this PulsarSinkBuilder.
     */
    public T setServiceUrl(String serviceUrl) {
        return setConfig(PULSAR_SERVICE_URL, serviceUrl);
    }

    /**
     * Set an arbitrary property for the PulsarSource and PulsarSink. The valid keys can be found
     * in {@link PulsarSourceOptions}, {@link PulsarSinkOptions} and {@link PulsarOptions}.
     *
     * <p>Make sure the option could be set only once or with same value.
     *
     * @param key   the key of the property.
     * @param value the value of the property.
     * @return this subclass object of PulsarBaseBuilder.
     */
    @SuppressWarnings("unchecked")
    public <E> T setConfig(ConfigOption<E> key, E value) {
        checkNotNull(key);
        checkNotNull(value);
        if (configuration.contains(key)) {
            E oldValue = configuration.get(key);
            checkArgument(
                    Objects.equals(oldValue, value),
                    "This option %s has been already set to value %s.",
                    key.key(),
                    oldValue);
        } else {
            configuration.set(key, value);
        }
        return (T) this;
    }

    /**
     * Set arbitrary properties for the PulsarSource and PulsarSink. The valid keys can be found
     * in {@link PulsarSourceOptions}, {@link PulsarSinkOptions} and {@link PulsarOptions}.
     *
     * @param config the config to set for the PulsarSource or PulsarSink.
     * @return this subclass object of PulsarBaseBuilder.
     */
    @SuppressWarnings("unchecked")
    public T setConfig(Configuration config) {
        Map<String, String> existedConfigs = configuration.toMap();
        List<String> duplicatedKeys = new ArrayList<>();
        for (Map.Entry<String, String> entry : config.toMap().entrySet()) {
            String key = entry.getKey();
            String value2 = existedConfigs.get(key);
            if (value2 != null && !value2.equals(entry.getValue())) {
                duplicatedKeys.add(key);
            }
        }
        checkArgument(
                duplicatedKeys.isEmpty(),
                "Invalid configuration, these keys %s are already exist with different config value.",
                duplicatedKeys);
        configuration.addAll(config);
        return (T) this;
    }
}
