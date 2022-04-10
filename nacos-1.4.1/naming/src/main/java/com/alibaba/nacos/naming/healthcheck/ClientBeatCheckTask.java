/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.naming.healthcheck;

import com.alibaba.nacos.common.utils.IPUtil;
import com.alibaba.nacos.common.http.Callback;
import com.alibaba.nacos.common.model.RestResult;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alibaba.nacos.sys.utils.ApplicationUtils;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.healthcheck.events.InstanceHeartbeatTimeoutEvent;
import com.alibaba.nacos.naming.misc.GlobalConfig;
import com.alibaba.nacos.naming.misc.HttpClient;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NamingProxy;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.push.PushService;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

/**
 * Check and update statues of ephemeral instances, remove them if they have been expired.
 *
 * @author nkorange
 */
public class ClientBeatCheckTask implements Runnable {

    private Service service;

    public ClientBeatCheckTask(Service service) {
        this.service = service;
    }

    @JsonIgnore
    public PushService getPushService() {
        return ApplicationUtils.getBean(PushService.class);
    }

    @JsonIgnore
    public DistroMapper getDistroMapper() {
        return ApplicationUtils.getBean(DistroMapper.class);
    }

    public GlobalConfig getGlobalConfig() {
        return ApplicationUtils.getBean(GlobalConfig.class);
    }

    public SwitchDomain getSwitchDomain() {
        return ApplicationUtils.getBean(SwitchDomain.class);
    }

    public String taskKey() {
        return KeyBuilder.buildServiceMetaKey(service.getNamespaceId(), service.getName());
    }

    @Override
    public void run() {
        try {
            // ! 集群的心跳, 对注册表服务名称进行hash然后取模定位, 不是这个范围内 直接return 不走心跳
            if (!getDistroMapper().responsible(service.getName())) {
                return;
            }

            if (!getSwitchDomain().isHealthCheckEnabled()) {
                return;
            }
            // ! 获取所有临时节点实例。
            List<Instance> instances = service.allIPs(true);

            // first set health status of instances:
            // # 循环遍历全部实例
            for (Instance instance : instances) {
                // # 系统当前时间 - 实例上一次心跳时间 > 实例配置的超时时间, 默认15秒
                if (System.currentTimeMillis() - instance.getLastBeat() > instance.getInstanceHeartBeatTimeOut()) {
                    if (!instance.isMarked()) {
                        if (instance.isHealthy()) {
                            instance.setHealthy(false);
                            Loggers.EVT_LOG
                                .info("{POS} {IP-DISABLED} valid: {}:{}@{}@{}, region: {}, msg: client timeout after {}, last beat: {}",
                                    instance.getIp(), instance.getPort(), instance.getClusterName(),
                                    service.getName(), UtilsAndCommons.LOCALHOST_SITE,
                                    instance.getInstanceHeartBeatTimeOut(), instance.getLastBeat());
                            // ! 修改服务状态 true改为false
                            getPushService().serviceChanged(service);
                            ApplicationUtils.publishEvent(new InstanceHeartbeatTimeoutEvent(this, instance));
                        }
                    }
                }
            }

            if (!getGlobalConfig().isExpireInstance()) {
                return;
            }

            // then remove obsolete instances:
            // # 循环所有的实例
            for (Instance instance : instances) {

                if (instance.isMarked()) {
                    continue;
                }
                // # 系统当前时间 - 实例上一次心跳时间 > 实例配置的超时删除时间, 默认15秒
                if (System.currentTimeMillis() - instance.getLastBeat() > instance.getIpDeleteTimeout()) {
                    // delete instance
                    Loggers.SRV_LOG.info("[AUTO-DELETE-IP] service: {}, ip: {}", service.getName(),
                        JacksonUtils.toJson(instance));
                    // ! 超过超时时间, 删除实例
                    deleteIp(instance);
                }
            }

        } catch (Exception e) {
            Loggers.SRV_LOG.warn("Exception while processing client beat time out.", e);
        }

    }

    private void deleteIp(Instance instance) {

        try {
            NamingProxy.Request request = NamingProxy.Request.newRequest();
            request.appendParam("ip", instance.getIp()).appendParam("port", String.valueOf(instance.getPort()))
                .appendParam("ephemeral", "true").appendParam("clusterName", instance.getClusterName())
                .appendParam("serviceName", service.getName()).appendParam("namespaceId", service.getNamespaceId());

            String url = "http://" + IPUtil.localHostIP() + IPUtil.IP_PORT_SPLITER + EnvUtil.getPort() + EnvUtil.getContextPath()
                + UtilsAndCommons.NACOS_NAMING_CONTEXT + "/instance?" + request.toUrl();

            // delete instance asynchronously:
            // ! 调接口删除
            HttpClient.asyncHttpDelete(url, null, null, new Callback<String>() {
                @Override
                public void onReceive(RestResult<String> result) {
                    if (!result.ok()) {
                        Loggers.SRV_LOG
                            .error("[IP-DEAD] failed to delete ip automatically, ip: {}, caused {}, resp code: {}",
                                instance.toJson(), result.getMessage(), result.getCode());
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    Loggers.SRV_LOG
                        .error("[IP-DEAD] failed to delete ip automatically, ip: {}, error: {}", instance.toJson(),
                            throwable);
                }

                @Override
                public void onCancel() {

                }
            });

        } catch (Exception e) {
            Loggers.SRV_LOG
                .error("[IP-DEAD] failed to delete ip automatically, ip: {}, error: {}", instance.toJson(), e);
        }
    }
}
