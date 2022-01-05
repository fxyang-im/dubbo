/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.common.config;

import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;
import org.apache.dubbo.common.context.FrameworkExt;
import org.apache.dubbo.common.context.LifecycleAdapter;
import org.apache.dubbo.common.extension.DisableInject;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.config.AbstractConfig;
import org.apache.dubbo.config.ConfigCenterConfig;
import org.apache.dubbo.config.context.ConfigConfigurationAdapter;
import org.apache.dubbo.config.context.ConfigManager;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Environment extends LifecycleAdapter implements FrameworkExt {
    private static final Logger logger = LoggerFactory.getLogger(Environment.class);
    public static final String NAME = "environment";

    private final PropertiesConfiguration propertiesConfiguration;
    private final SystemConfiguration systemConfiguration;
    private final EnvironmentConfiguration environmentConfiguration;
    private final InmemoryConfiguration externalConfiguration;
    private final InmemoryConfiguration appExternalConfiguration;

    private CompositeConfiguration globalConfiguration;
    private CompositeConfiguration dynamicGlobalConfiguration;


    private Map<String, String> externalConfigurationMap = new HashMap<>();
    private Map<String, String> appExternalConfigurationMap = new HashMap<>();

    private boolean configCenterFirst = true;

    private DynamicConfiguration dynamicConfiguration;

    public Environment() {
        this.propertiesConfiguration = new PropertiesConfiguration();
        this.systemConfiguration = new SystemConfiguration();
        this.environmentConfiguration = new EnvironmentConfiguration();
        this.externalConfiguration = new InmemoryConfiguration();
        this.appExternalConfiguration = new InmemoryConfiguration();
    }

    @Override
    public void initialize() throws IllegalStateException {
        ConfigManager configManager = ApplicationModel.getConfigManager();
        Optional<Collection<ConfigCenterConfig>> defaultConfigs = configManager.getDefaultConfigCenter();
        defaultConfigs.ifPresent(configs -> {
            for (ConfigCenterConfig config : configs) {
                this.setExternalConfigMap(config.getExternalConfiguration());
                this.setAppExternalConfigMap(config.getAppExternalConfiguration());
            }
        });

        this.externalConfiguration.setProperties(externalConfigurationMap);
        this.appExternalConfiguration.setProperties(appExternalConfigurationMap);
    }

    @DisableInject
    public void setExternalConfigMap(Map<String, String> externalConfiguration) {
        if (externalConfiguration != null) {
            this.externalConfigurationMap = externalConfiguration;
        }
    }

    @DisableInject
    public void setAppExternalConfigMap(Map<String, String> appExternalConfiguration) {
        if (appExternalConfiguration != null) {
            this.appExternalConfigurationMap = appExternalConfiguration;
        }
    }

    public Map<String, String> getExternalConfigurationMap() {
        return externalConfigurationMap;
    }

    public Map<String, String> getAppExternalConfigurationMap() {
        return appExternalConfigurationMap;
    }

    public void updateExternalConfigurationMap(Map<String, String> externalMap) {
        this.externalConfigurationMap.putAll(externalMap);
    }

    public void updateAppExternalConfigurationMap(Map<String, String> externalMap) {
        this.appExternalConfigurationMap.putAll(externalMap);
    }

    /**
     *
     * 在dubbo启动时，会加载各种配置：
     * 应用配置ApplicationConfig
     * 注册配置RegisterConfig
     * 服务配置：ServiceConfig
     * 消费者配置：ConsumerConfig
     * 提供者配置：ProviderConfig
     * 方法配置：MethodConfig
     * 参数配置：ArgumentConfig
     * 所有的配置信息都会添加到一个复合引用中。
     * At start-up, Dubbo is driven by various configuration, such as Application, Registry, Protocol, etc.
     * All configurations will be converged into a data bus - URL, and then drive the subsequent process.
     * <p>
     * 配置的来源有很多
     * xml，properties，api，系统环境变量，jvm参数，启动参数，配置中心等
     * 这个方法通过自身定义的优先级来剔除重复设置的变量
     * At present, there are many configuration sources, including AbstractConfig (API, XML, annotation), - D, config center, etc.
     * This method helps us to filter out the most priority values from various configuration sources.
     *
     * @param config
     * @return
     */
    public synchronized CompositeConfiguration getPrefixedConfiguration(AbstractConfig config) {
        CompositeConfiguration prefixedConfiguration = new CompositeConfiguration(config.getPrefix(), config.getId());
        Configuration configuration = new ConfigConfigurationAdapter(config);
        // 配置中心优先
        if (this.isConfigCenterFirst()) {
            // The sequence would be:
            // SystemConfiguration
            // -> AppExternalConfiguration
            // -> ExternalConfiguration
            // -> AbstractConfig
            // -> PropertiesConfiguration
            // Config center has the highest priority 配置中心拥有最高级的权限
            // 系统配置
            prefixedConfiguration.addConfiguration(systemConfiguration);
            // 环境配置
            prefixedConfiguration.addConfiguration(environmentConfiguration);
            // 应用扩展配置
            prefixedConfiguration.addConfiguration(appExternalConfiguration);
            // 扩展配置
            prefixedConfiguration.addConfiguration(externalConfiguration);
            // 基本配置
            prefixedConfiguration.addConfiguration(configuration);
            // 属性配置
            prefixedConfiguration.addConfiguration(propertiesConfiguration);
        } else {
            // The sequence would be: SystemConfiguration -> AbstractConfig -> AppExternalConfiguration -> ExternalConfiguration -> PropertiesConfiguration
            // Config center has the highest priority
            prefixedConfiguration.addConfiguration(systemConfiguration);
            prefixedConfiguration.addConfiguration(environmentConfiguration);
            prefixedConfiguration.addConfiguration(configuration);
            prefixedConfiguration.addConfiguration(appExternalConfiguration);
            prefixedConfiguration.addConfiguration(externalConfiguration);
            prefixedConfiguration.addConfiguration(propertiesConfiguration);
        }
        return prefixedConfiguration;
    }

    /**
     * There are two ways to get configuration during exposure / reference or at runtime:
     * 1. URL, The value in the URL is relatively fixed. we can get value directly.
     * 2. The configuration exposed in this method is convenient for us to query the latest values from multiple
     * prioritized sources, it also guarantees that configs changed dynamically can take effect on the fly.
     */
    public Configuration getConfiguration() {
        if (globalConfiguration == null) {
            globalConfiguration = new CompositeConfiguration();
            globalConfiguration.addConfiguration(systemConfiguration);
            globalConfiguration.addConfiguration(environmentConfiguration);
            globalConfiguration.addConfiguration(appExternalConfiguration);
            globalConfiguration.addConfiguration(externalConfiguration);
            globalConfiguration.addConfiguration(propertiesConfiguration);
        }
        return globalConfiguration;
    }

    public Configuration getDynamicGlobalConfiguration() {
        if (dynamicGlobalConfiguration == null) {
            if (dynamicConfiguration == null) {
                if (logger.isWarnEnabled()) {
                    logger.warn("dynamicConfiguration is null , return globalConfiguration.");
                }
                return globalConfiguration;
            }
            dynamicGlobalConfiguration = new CompositeConfiguration();
            dynamicGlobalConfiguration.addConfiguration(dynamicConfiguration);
            dynamicGlobalConfiguration.addConfiguration(getConfiguration());
        }
        return dynamicGlobalConfiguration;
    }

    public boolean isConfigCenterFirst() {
        return configCenterFirst;
    }

    @DisableInject
    public void setConfigCenterFirst(boolean configCenterFirst) {
        this.configCenterFirst = configCenterFirst;
    }

    public Optional<DynamicConfiguration> getDynamicConfiguration() {
        return Optional.ofNullable(dynamicConfiguration);
    }

    @DisableInject
    public void setDynamicConfiguration(DynamicConfiguration dynamicConfiguration) {
        this.dynamicConfiguration = dynamicConfiguration;
    }

    @Override
    public void destroy() throws IllegalStateException {
        clearExternalConfigs();
        clearAppExternalConfigs();
        clearDynamicConfiguration();
    }

    public PropertiesConfiguration getPropertiesConfiguration() {
        return propertiesConfiguration;
    }

    public SystemConfiguration getSystemConfiguration() {
        return systemConfiguration;
    }

    public EnvironmentConfiguration getEnvironmentConfiguration() {
        return environmentConfiguration;
    }

    public InmemoryConfiguration getExternalConfiguration() {
        return externalConfiguration;
    }

    public InmemoryConfiguration getAppExternalConfiguration() {
        return appExternalConfiguration;
    }

    // For test
    public void clearExternalConfigs() {
        this.externalConfiguration.clear();
        this.externalConfigurationMap.clear();
    }

    // For test
    public void clearAppExternalConfigs() {
        this.appExternalConfiguration.clear();
        this.appExternalConfigurationMap.clear();
    }

    public void clearDynamicConfiguration() {
        this.dynamicConfiguration = null;
    }
}
