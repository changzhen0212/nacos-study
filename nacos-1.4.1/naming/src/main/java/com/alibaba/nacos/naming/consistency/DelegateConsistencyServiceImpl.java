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

package com.alibaba.nacos.naming.consistency;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.naming.consistency.ephemeral.EphemeralConsistencyService;
import com.alibaba.nacos.naming.consistency.persistent.PersistentConsistencyServiceDelegateImpl;
import com.alibaba.nacos.naming.pojo.Record;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;

/**
 * Consistency delegate.
 *
 * @author nkorange
 * @since 1.0.0
 */
@DependsOn("ProtocolManager")
@Service("consistencyDelegate")
public class DelegateConsistencyServiceImpl implements ConsistencyService {

    private final PersistentConsistencyServiceDelegateImpl persistentConsistencyService;

    private final EphemeralConsistencyService ephemeralConsistencyService;

    public DelegateConsistencyServiceImpl(PersistentConsistencyServiceDelegateImpl persistentConsistencyService,
            EphemeralConsistencyService ephemeralConsistencyService) {
        this.persistentConsistencyService = persistentConsistencyService;
        this.ephemeralConsistencyService = ephemeralConsistencyService;
    }

    @Override
    public void put(String key, Record value) throws NacosException {
        // ! 往下走 根据`mapConsistencyService(key)` 返回的对象, 确认具体进哪个实现方法,
        // ! AP架构,实现Distro协议,最终确认到是DistroConsistencyServiceImpl, 进入它的put方法
        // ! CP架构是Raft协议, 进入 RaftConsistencyServiceImpl#put方法
        // # key 是刚才上一步生成的字符串, Record是instance的接口
        mapConsistencyService(key).put(key, value);
    }

    @Override
    public void remove(String key) throws NacosException {
        mapConsistencyService(key).remove(key);
    }

    @Override
    public Datum get(String key) throws NacosException {
        return mapConsistencyService(key).get(key);
    }

    @Override
    public void listen(String key, RecordListener listener) throws NacosException {

        // this special key is listened by both:
        if (KeyBuilder.SERVICE_META_KEY_PREFIX.equals(key)) {
            persistentConsistencyService.listen(key, listener);
            ephemeralConsistencyService.listen(key, listener);
            return;
        }

        mapConsistencyService(key).listen(key, listener);
    }

    @Override
    public void unListen(String key, RecordListener listener) throws NacosException {
        mapConsistencyService(key).unListen(key, listener);
    }

    @Override
    public boolean isAvailable() {
        return ephemeralConsistencyService.isAvailable() && persistentConsistencyService.isAvailable();
    }

    private ConsistencyService mapConsistencyService(String key) {
        // ! matchEphemeralKey 跟进 ,判断key的类型,最终确认走AP还是CP
        // # ephemeralConsistencyService 临时实例 AP架构
        // # persistentConsistencyService 持久实例 CP架构
        // # 进入 ephemeralConsistencyService 接口, 看他的实现, 可以找到DistroConsistencyServiceImpl, 返回上层调用就可以确认进入哪个接口了
        return KeyBuilder.matchEphemeralKey(key) ? ephemeralConsistencyService : persistentConsistencyService;
    }
}
