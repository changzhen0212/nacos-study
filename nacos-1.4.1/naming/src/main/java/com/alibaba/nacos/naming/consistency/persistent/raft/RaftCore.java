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

package com.alibaba.nacos.naming.consistency.persistent.raft;

import com.alibaba.nacos.common.utils.IPUtil;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.exception.runtime.NacosRuntimeException;
import com.alibaba.nacos.common.http.Callback;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.model.RestResult;
import com.alibaba.nacos.common.notify.EventPublisher;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.consistency.DataOperation;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.ValueChangeEvent;
import com.alibaba.nacos.naming.consistency.persistent.ClusterVersionJudgement;
import com.alibaba.nacos.naming.consistency.persistent.PersistentNotifier;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.GlobalConfig;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.HttpClient;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NetUtils;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.monitor.MetricsMonitor;
import com.alibaba.nacos.naming.pojo.Record;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.context.annotation.DependsOn;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

/**
 * Raft core code.
 *
 * @author nacos
 * @deprecated will remove in 1.4.x
 */
@Deprecated
@DependsOn("ProtocolManager")
@Component
public class RaftCore implements Closeable {

    public static final String API_VOTE = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/vote";

    public static final String API_BEAT = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/beat";

    public static final String API_PUB = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";

    public static final String API_DEL = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";

    public static final String API_GET = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";

    public static final String API_ON_PUB = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum/commit";

    public static final String API_ON_DEL = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum/commit";

    public static final String API_GET_PEER = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/peer";

    public static final Lock OPERATE_LOCK = new ReentrantLock();

    public static final int PUBLISH_TERM_INCREASE_COUNT = 100;

    private volatile ConcurrentMap<String, List<RecordListener>> listeners = new ConcurrentHashMap<>();

    private volatile ConcurrentMap<String, Datum> datums = new ConcurrentHashMap<>();

    private RaftPeerSet peers;

    private final SwitchDomain switchDomain;

    private final GlobalConfig globalConfig;

    private final RaftProxy raftProxy;

    private final RaftStore raftStore;

    private final ClusterVersionJudgement versionJudgement;

    public final PersistentNotifier notifier;

    private final EventPublisher publisher;

    private final RaftListener raftListener;

    private boolean initialized = false;

    private volatile boolean stopWork = false;

    private ScheduledFuture masterTask = null;

    private ScheduledFuture heartbeatTask = null;

    public RaftCore(RaftPeerSet peers, SwitchDomain switchDomain, GlobalConfig globalConfig, RaftProxy raftProxy,
            RaftStore raftStore, ClusterVersionJudgement versionJudgement, RaftListener raftListener) {
        this.peers = peers;
        this.switchDomain = switchDomain;
        this.globalConfig = globalConfig;
        this.raftProxy = raftProxy;
        this.raftStore = raftStore;
        this.versionJudgement = versionJudgement;
        this.notifier = new PersistentNotifier(key -> null == getDatum(key) ? null : getDatum(key).value);
        this.publisher = NotifyCenter.registerToPublisher(ValueChangeEvent.class, 16384);
        this.raftListener = raftListener;
    }

    /**
     * Init raft core.
     *
     * @throws Exception any exception during init
     */
    @PostConstruct
    public void init() throws Exception {
        Loggers.RAFT.info("initializing Raft sub-system");
        final long start = System.currentTimeMillis();
        // ! 从磁盘加载数据
        raftStore.loadDatums(notifier, datums);

        setTerm(NumberUtils.toLong(raftStore.loadMeta().getProperty("term"), 0L));

        Loggers.RAFT.info("cache loaded, datum count: {}, current term: {}", datums.size(), peers.getTerm());

        initialized = true;

        Loggers.RAFT.info("finish to load data from disk, cost: {} ms.", (System.currentTimeMillis() - start));

        // ! 注册两个任务
        // ! 注册选举任务 500毫秒一次 ,进任务查看,看run方法
        masterTask = GlobalExecutor.registerMasterElection(new MasterElection());
        // ! 注册心跳任务 500毫秒一次 ,进任务查看,看run方法
        heartbeatTask = GlobalExecutor.registerHeartbeat(new HeartBeat());

        versionJudgement.registerObserver(isAllNewVersion -> {
            stopWork = isAllNewVersion;
            if (stopWork) {
                try {
                    shutdown();
                    raftListener.removeOldRaftMetadata();
                } catch (NacosException e) {
                    throw new NacosRuntimeException(NacosException.SERVER_ERROR, e);
                }
            }
        }, 100);

        NotifyCenter.registerSubscriber(notifier);

        Loggers.RAFT.info("timer started: leader timeout ms: {}, heart-beat timeout ms: {}",
                GlobalExecutor.LEADER_TIMEOUT_MS, GlobalExecutor.HEARTBEAT_INTERVAL_MS);
    }

    public Map<String, ConcurrentHashSet<RecordListener>> getListeners() {
        return notifier.getListeners();
    }

    /**
     * Signal publish new record. If not leader, signal to leader. If leader, try to commit publish.
     *
     * @param key   key
     * @param value value
     * @throws Exception any exception during publish
     */
    public void signalPublish(String key, Record value) throws Exception {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        if (!isLeader()) { // # 如果当前不是leader
            ObjectNode params = JacksonUtils.createEmptyJsonNode();
            params.put("key", key);
            params.replace("value", JacksonUtils.transferToJsonNode(value));
            Map<String, String> parameters = new HashMap<>(1);
            parameters.put("key", key);

            final RaftPeer leader = getLeader();
            // ! http调用api raft/datum
            raftProxy.proxyPostLarge(leader.ip, API_PUB, params.toString(), parameters);
            return;
        }

        OPERATE_LOCK.lock();
        try {
            final long start = System.currentTimeMillis();
            final Datum datum = new Datum();
            datum.key = key;
            datum.value = value;
            if (getDatum(key) == null) {
                datum.timestamp.set(1L);
            } else {
                datum.timestamp.set(getDatum(key).timestamp.incrementAndGet());
            }

            ObjectNode json = JacksonUtils.createEmptyJsonNode();
            json.replace("datum", JacksonUtils.transferToJsonNode(datum));
            json.replace("source", JacksonUtils.transferToJsonNode(peers.local()));
            // ! 发布事件
            onPublish(datum, peers.local());

            final String content = json.toString();

            // ! 创建倒计时 majorityCount 半数以上算法
            final CountDownLatch latch = new CountDownLatch(peers.majorityCount());
            // ! 遍历全部的服务节点, 包括自己
            for (final String server : peers.allServersIncludeMyself()) {
                // # 如果是leader, 减一 继续
                if (isLeader(server)) {
                    latch.countDown();
                    continue;
                }
                // # 如果不是leader,发送消息给其他节点 /raft/datum/commit
                final String url = buildUrl(server, API_ON_PUB);
                HttpClient.asyncHttpPostLarge(url, Arrays.asList("key", key), content, new Callback<String>() {
                    @Override
                    public void onReceive(RestResult<String> result) {
                        if (!result.ok()) {
                            Loggers.RAFT
                                    .warn("[RAFT] failed to publish data to peer, datumId={}, peer={}, http code={}",
                                            datum.key, server, result.getCode());
                            return;
                        }
                        latch.countDown();
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        Loggers.RAFT.error("[RAFT] failed to publish data to peer", throwable);
                    }

                    @Override
                    public void onCancel() {

                    }
                });

            }

            if (!latch.await(UtilsAndCommons.RAFT_PUBLISH_TIMEOUT, TimeUnit.MILLISECONDS)) {
                // only majority servers return success can we consider this update success
                Loggers.RAFT.error("data publish failed, caused failed to notify majority, key={}", key);
                throw new IllegalStateException("data publish failed, caused failed to notify majority, key=" + key);
            }

            long end = System.currentTimeMillis();
            Loggers.RAFT.info("signalPublish cost {} ms, key: {}", (end - start), key);
        } finally {
            OPERATE_LOCK.unlock();
        }
    }

    /**
     * Signal delete record. If not leader, signal leader delete. If leader, try to commit delete.
     *
     * @param key key
     * @throws Exception any exception during delete
     */
    public void signalDelete(final String key) throws Exception {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        OPERATE_LOCK.lock();
        try {

            if (!isLeader()) {
                Map<String, String> params = new HashMap<>(1);
                params.put("key", URLEncoder.encode(key, "UTF-8"));
                raftProxy.proxy(getLeader().ip, API_DEL, params, HttpMethod.DELETE);
                return;
            }

            // construct datum:
            Datum datum = new Datum();
            datum.key = key;
            ObjectNode json = JacksonUtils.createEmptyJsonNode();
            json.replace("datum", JacksonUtils.transferToJsonNode(datum));
            json.replace("source", JacksonUtils.transferToJsonNode(peers.local()));

            onDelete(datum.key, peers.local());

            for (final String server : peers.allServersWithoutMySelf()) {
                String url = buildUrl(server, API_ON_DEL);
                HttpClient.asyncHttpDeleteLarge(url, null, json.toString(), new Callback<String>() {
                    @Override
                    public void onReceive(RestResult<String> result) {
                        if (!result.ok()) {
                            Loggers.RAFT
                                    .warn("[RAFT] failed to delete data from peer, datumId={}, peer={}, http code={}",
                                            key, server, result.getCode());
                            return;
                        }

                        RaftPeer local = peers.local();

                        local.resetLeaderDue();
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        Loggers.RAFT.error("[RAFT] failed to delete data from peer", throwable);
                    }

                    @Override
                    public void onCancel() {

                    }
                });
            }
        } finally {
            OPERATE_LOCK.unlock();
        }
    }

    /**
     * Do publish. If leader, commit publish to store. If not leader, stop publish because should signal to leader.
     *
     * @param datum  datum
     * @param source source raft peer
     * @throws Exception any exception during publish
     */
    public void onPublish(Datum datum, RaftPeer source) throws Exception {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        RaftPeer local = peers.local();
        if (datum.value == null) {
            Loggers.RAFT.warn("received empty datum");
            throw new IllegalStateException("received empty datum");
        }

        if (!peers.isLeader(source.ip)) {
            Loggers.RAFT
                    .warn("peer {} tried to publish data but wasn't leader, leader: {}", JacksonUtils.toJson(source),
                            JacksonUtils.toJson(getLeader()));
            throw new IllegalStateException("peer(" + source.ip + ") tried to publish " + "data but wasn't leader");
        }

        if (source.term.get() < local.term.get()) {
            Loggers.RAFT.warn("out of date publish, pub-term: {}, cur-term: {}", JacksonUtils.toJson(source),
                    JacksonUtils.toJson(local));
            throw new IllegalStateException(
                    "out of date publish, pub-term:" + source.term.get() + ", cur-term: " + local.term.get());
        }

        local.resetLeaderDue();

        // if data should be persisted, usually this is true:
        if (KeyBuilder.matchPersistentKey(datum.key)) {
            // ! raft存储写数据
            raftStore.write(datum);
        }

        datums.put(datum.key, datum);

        if (isLeader()) {
            local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT);
        } else {
            if (local.term.get() + PUBLISH_TERM_INCREASE_COUNT > source.term.get()) {
                //set leader term:
                getLeader().term.set(source.term.get());
                local.term.set(getLeader().term.get());
            } else {
                local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT);
            }
        }
        raftStore.updateTerm(local.term.get());
        // ! 发布事件
        NotifyCenter.publishEvent(ValueChangeEvent.builder().key(datum.key).action(DataOperation.CHANGE).build());
        Loggers.RAFT.info("data added/updated, key={}, term={}", datum.key, local.term);
    }

    /**
     * Do delete. If leader, commit delete to store. If not leader, stop delete because should signal to leader.
     *
     * @param datumKey datum key
     * @param source   source raft peer
     * @throws Exception any exception during delete
     */
    public void onDelete(String datumKey, RaftPeer source) throws Exception {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        RaftPeer local = peers.local();

        if (!peers.isLeader(source.ip)) {
            Loggers.RAFT
                    .warn("peer {} tried to publish data but wasn't leader, leader: {}", JacksonUtils.toJson(source),
                            JacksonUtils.toJson(getLeader()));
            throw new IllegalStateException("peer(" + source.ip + ") tried to publish data but wasn't leader");
        }

        if (source.term.get() < local.term.get()) {
            Loggers.RAFT.warn("out of date publish, pub-term: {}, cur-term: {}", JacksonUtils.toJson(source),
                    JacksonUtils.toJson(local));
            throw new IllegalStateException(
                    "out of date publish, pub-term:" + source.term + ", cur-term: " + local.term);
        }

        local.resetLeaderDue();

        // do apply
        String key = datumKey;
        deleteDatum(key);

        if (KeyBuilder.matchServiceMetaKey(key)) {

            if (local.term.get() + PUBLISH_TERM_INCREASE_COUNT > source.term.get()) {
                //set leader term:
                getLeader().term.set(source.term.get());
                local.term.set(getLeader().term.get());
            } else {
                local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT);
            }

            raftStore.updateTerm(local.term.get());
        }

        Loggers.RAFT.info("data removed, key={}, term={}", datumKey, local.term);

    }

    @Override
    public void shutdown() throws NacosException {
        this.stopWork = true;
        this.raftStore.shutdown();
        this.peers.shutdown();
        Loggers.RAFT.warn("start to close old raft protocol!!!");
        Loggers.RAFT.warn("stop old raft protocol task for notifier");
        NotifyCenter.deregisterSubscriber(notifier);
        Loggers.RAFT.warn("stop old raft protocol task for master task");
        masterTask.cancel(true);
        Loggers.RAFT.warn("stop old raft protocol task for heartbeat task");
        heartbeatTask.cancel(true);
        Loggers.RAFT.warn("clean old cache datum for old raft");
        datums.clear();
    }

    public class MasterElection implements Runnable {

        @Override
        public void run() {
            try {
                if (stopWork) {
                    return;
                }
                if (!peers.isReady()) {
                    return;
                }
                // # 随机休眠
                RaftPeer local = peers.local();
                local.leaderDueMs -= GlobalExecutor.TICK_PERIOD_MS;

                if (local.leaderDueMs > 0) {
                    return;
                }

                // reset timeout
                // # 重置leader信息, 把数据做一些初始化0值之类的
                local.resetLeaderDue();
                // # 重置心跳信息
                local.resetHeartbeatDue();

                // ! 发送投票
                sendVote();
            } catch (Exception e) {
                Loggers.RAFT.warn("[RAFT] error while master election {}", e);
            }

        }
        // ! 发送投票
        private void sendVote() {

            RaftPeer local = peers.get(NetUtils.localServer());
            Loggers.RAFT.info("leader timeout, start voting,leader: {}, term: {}", JacksonUtils.toJson(getLeader()),
                    local.term);
            // # peers-节点信息,
            // # 重置节点信息
            peers.reset();
            // # term-选举周期
            // # 选举周期+1
            local.term.incrementAndGet();
            // # voteFor-投给谁
            // # 此处投给自己
            local.voteFor = local.ip;
            // # state-状态
            // # 把自己的状态改为候选者
            local.state = RaftPeer.State.CANDIDATE;

            Map<String, String> params = new HashMap<>(1);
            params.put("vote", JacksonUtils.toJson(local));
            // # 遍历全部服务节点, 除去自身
            for (final String server : peers.allServersWithoutMySelf()) {
                // # 构建投票的url /v1/ns/raft/vote
                final String url = buildUrl(server, API_VOTE);
                try {
                    // # http调用url
                    HttpClient.asyncHttpPost(url, null, params, new Callback<String>() {
                        // # 回调
                        @Override
                        public void onReceive(RestResult<String> result) {
                            if (!result.ok()) {
                                Loggers.RAFT.error("NACOS-RAFT vote failed: {}, url: {}", result.getCode(), url);
                                return;
                            }
                            // # 获取返回数据, 包含其他节点的投票信息
                            RaftPeer peer = JacksonUtils.toObj(result.getData(), RaftPeer.class);

                            Loggers.RAFT.info("received approve from peer: {}", JacksonUtils.toJson(peer));
                            // ! 解析响应数据, 选出leader, 判断自己是否超过了半数选票
                            peers.decideLeader(peer);

                        }

                        @Override
                        public void onError(Throwable throwable) {
                            Loggers.RAFT.error("error while sending vote to server: {}", server, throwable);
                        }

                        @Override
                        public void onCancel() {

                        }
                    });
                } catch (Exception e) {
                    Loggers.RAFT.warn("error while sending vote to server: {}", server);
                }
            }
        }
    }

    /**
     * Received vote.
     *
     * @param remote remote raft peer of vote information
     * @return self-peer information
     */
    public synchronized RaftPeer receivedVote(RaftPeer remote) {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        if (!peers.contains(remote)) {
            throw new IllegalStateException("can not find peer: " + remote.ip);
        }

        RaftPeer local = peers.get(NetUtils.localServer());
        if (remote.term.get() <= local.term.get()) {
            String msg = "received illegitimate vote" + ", voter-term:" + remote.term + ", votee-term:" + local.term;

            Loggers.RAFT.info(msg);
            if (StringUtils.isEmpty(local.voteFor)) {
                local.voteFor = local.ip;
            }

            return local;
        }

        local.resetLeaderDue();

        local.state = RaftPeer.State.FOLLOWER;
        local.voteFor = remote.ip;
        local.term.set(remote.term.get());

        Loggers.RAFT.info("vote {} as leader, term: {}", remote.ip, remote.term);

        return local;
    }

    public class HeartBeat implements Runnable {

        @Override
        public void run() {
            try {
                if (stopWork) {
                    return;
                }
                if (!peers.isReady()) {
                    return;
                }
                // # 随机休眠时间
                RaftPeer local = peers.local();
                local.heartbeatDueMs -= GlobalExecutor.TICK_PERIOD_MS;
                if (local.heartbeatDueMs > 0) {
                    return;
                }
                // # 初始化数据
                local.resetHeartbeatDue();
                // ! 发送心跳
                sendBeat();
            } catch (Exception e) {
                Loggers.RAFT.warn("[RAFT] error while sending beat {}", e);
            }

        }
        // ! 发送心跳
        private void sendBeat() throws IOException, InterruptedException {
            // # 获取当前节点
            RaftPeer local = peers.local();
            // # 当前节点如果不是leader,直接return 不能发心跳
            if (EnvUtil.getStandaloneMode() || local.state != RaftPeer.State.LEADER) {
                return;
            }
            if (Loggers.RAFT.isDebugEnabled()) {
                Loggers.RAFT.debug("[RAFT] send beat with {} keys.", datums.size());
            }

            local.resetLeaderDue();

            // build data
            ObjectNode packet = JacksonUtils.createEmptyJsonNode();
            packet.replace("peer", JacksonUtils.transferToJsonNode(local));

            ArrayNode array = JacksonUtils.createEmptyArrayNode();

            if (switchDomain.isSendBeatOnly()) {
                Loggers.RAFT.info("[SEND-BEAT-ONLY] {}", switchDomain.isSendBeatOnly());
            }

            if (!switchDomain.isSendBeatOnly()) {
                // # 从缓存中获取数据
                for (Datum datum : datums.values()) {
                    // # element中保存着 datum的key和时间戳
                    ObjectNode element = JacksonUtils.createEmptyJsonNode();

                    if (KeyBuilder.matchServiceMetaKey(datum.key)) {
                        element.put("key", KeyBuilder.briefServiceMetaKey(datum.key));
                    } else if (KeyBuilder.matchInstanceListKey(datum.key)) {
                        element.put("key", KeyBuilder.briefInstanceListkey(datum.key));
                    }
                    element.put("timestamp", datum.timestamp.get());
                    // # 节点key-时间戳 添加到数组
                    array.add(element);
                }
            }
            // # 把节点key-时间戳的数组构建成一个packet
            packet.replace("datums", array);
            // broadcast
            Map<String, String> params = new HashMap<String, String>(1);
            params.put("beat", JacksonUtils.toJson(packet));

            String content = JacksonUtils.toJson(params);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            // # 压缩输出流
            GZIPOutputStream gzip = new GZIPOutputStream(out);
            gzip.write(content.getBytes(StandardCharsets.UTF_8));
            gzip.close();

            byte[] compressedBytes = out.toByteArray();
            String compressedContent = new String(compressedBytes, StandardCharsets.UTF_8);

            if (Loggers.RAFT.isDebugEnabled()) {
                Loggers.RAFT.debug("raw beat data size: {}, size of compressed data: {}", content.length(),
                        compressedContent.length());
            }
            // # 遍历全部服务节点,排除自身
            for (final String server : peers.allServersWithoutMySelf()) {
                try {
                    // # 构建心跳url v1/ns/raft/beat
                    final String url = buildUrl(server, API_BEAT);
                    if (Loggers.RAFT.isDebugEnabled()) {
                        Loggers.RAFT.debug("send beat to server " + server);
                    }
                    // # 压缩后的数据发送给其他所有节点
                    HttpClient.asyncHttpPostLarge(url, null, compressedBytes, new Callback<String>() {
                        @Override
                        public void onReceive(RestResult<String> result) {
                            if (!result.ok()) {
                                Loggers.RAFT.error("NACOS-RAFT beat failed: {}, peer: {}", result.getCode(), server);
                                MetricsMonitor.getLeaderSendBeatFailedException().increment();
                                return;
                            }

                            peers.update(JacksonUtils.toObj(result.getData(), RaftPeer.class));
                            if (Loggers.RAFT.isDebugEnabled()) {
                                Loggers.RAFT.debug("receive beat response from: {}", url);
                            }
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            Loggers.RAFT.error("NACOS-RAFT error while sending heart-beat to peer: {} {}", server,
                                    throwable);
                            MetricsMonitor.getLeaderSendBeatFailedException().increment();
                        }

                        @Override
                        public void onCancel() {

                        }
                    });
                } catch (Exception e) {
                    Loggers.RAFT.error("error while sending heart-beat to peer: {} {}", server, e);
                    MetricsMonitor.getLeaderSendBeatFailedException().increment();
                }
            }

        }
    }

    /**
     * Received beat from leader. // TODO split method to multiple smaller method.
     *
     * @param beat beat information from leader
     * @return self-peer information
     * @throws Exception any exception during handle
     */
    public RaftPeer receivedBeat(JsonNode beat) throws Exception {
        if (stopWork) {
            throw new IllegalStateException("old raft protocol already stop work");
        }
        final RaftPeer local = peers.local();
        final RaftPeer remote = new RaftPeer();
        // # 解析发送来的数据
        JsonNode peer = beat.get("peer");
        remote.ip = peer.get("ip").asText();
        remote.state = RaftPeer.State.valueOf(peer.get("state").asText());
        remote.term.set(peer.get("term").asLong());
        remote.heartbeatDueMs = peer.get("heartbeatDueMs").asLong();
        remote.leaderDueMs = peer.get("leaderDueMs").asLong();
        remote.voteFor = peer.get("voteFor").asText();
        // # 心跳包不是leader发送的,抛出异常
        if (remote.state != RaftPeer.State.LEADER) {
            Loggers.RAFT.info("[RAFT] invalid state from master, state: {}, remote peer: {}", remote.state,
                    JacksonUtils.toJson(remote));
            throw new IllegalArgumentException("invalid state from master, state: " + remote.state);
        }

        if (local.term.get() > remote.term.get()) {
            Loggers.RAFT
                    .info("[RAFT] out of date beat, beat-from-term: {}, beat-to-term: {}, remote peer: {}, and leaderDueMs: {}",
                            remote.term.get(), local.term.get(), JacksonUtils.toJson(remote), local.leaderDueMs);
            throw new IllegalArgumentException(
                    "out of date beat, beat-from-term: " + remote.term.get() + ", beat-to-term: " + local.term.get());
        }

        // # 如果之前不是follower, 把自己改成follower, 因为收到心跳了,说明自己不是leader, zk也有类似的操作
        if (local.state != RaftPeer.State.FOLLOWER) {

            Loggers.RAFT.info("[RAFT] make remote as leader, remote peer: {}", JacksonUtils.toJson(remote));
            // mk follower
            local.state = RaftPeer.State.FOLLOWER;
            local.voteFor = remote.ip;
        }
        // # 获取发来的 datums, 每个datums里是key-时间戳
        final JsonNode beatDatums = beat.get("datums");
        local.resetLeaderDue();
        local.resetHeartbeatDue();
        // !
        peers.makeLeader(remote);

        if (!switchDomain.isSendBeatOnly()) {
            // # 初始化receivedKeysMap
            Map<String, Integer> receivedKeysMap = new HashMap<>(datums.size());
            // # 把本地的datum.key放进去, value为0, value=0是本地数据,如果是1是远端数据
            for (Map.Entry<String, Datum> entry : datums.entrySet()) {
                receivedKeysMap.put(entry.getKey(), 0);
            }

            // now check datums
            List<String> batch = new ArrayList<>();

            // # processedCount= 每次遍历远端+1
            int processedCount = 0;
            if (Loggers.RAFT.isDebugEnabled()) {
                Loggers.RAFT
                        .debug("[RAFT] received beat with {} keys, RaftCore.datums' size is {}, remote server: {}, term: {}, local term: {}",
                                beatDatums.size(), datums.size(), remote.ip, remote.term, local.term);
            }
            // # 遍历 远端 发来的 beatDatums, 每个datums里是key-时间戳
            for (Object object : beatDatums) {
                processedCount = processedCount + 1;

                JsonNode entry = (JsonNode) object;
                String key = entry.get("key").asText();
                final String datumKey;

                if (KeyBuilder.matchServiceMetaKey(key)) {
                    datumKey = KeyBuilder.detailServiceMetaKey(key);
                } else if (KeyBuilder.matchInstanceListKey(key)) {
                    datumKey = KeyBuilder.detailInstanceListkey(key);
                } else {
                    // ignore corrupted key:
                    continue;
                }

                long timestamp = entry.get("timestamp").asLong();
                // # 把key放入map, 这个map在初始化时,会把本地的key都放进去, 本地数据value=0, 远端数据value=1
                receivedKeysMap.put(datumKey, 1);

                try {
                    // # 此处的datums是从节点自身的datums, 除了leader传来的,还有自己本地的datums,所以要判断
                    // # 如果本地的datum包含远端传来的datumkey. 且,本地的时间戳,大于远端传来的时间戳
                    if (datums.containsKey(datumKey) && datums.get(datumKey).timestamp.get() >= timestamp
                            // # 且 处理的数量 小于 远端发来的心跳datum数量 ,则不处理
                            // # processedCount= 每次遍历远端+1, beatDatums= 远端心跳size
                            && processedCount < beatDatums.size()) {
                        continue;
                    }
                    // # 上一个判断条件取反, 也就是, 本地不包含远端的datumKey, 或者 本地的时间戳 小于远端的时间戳
                    if (!(datums.containsKey(datumKey) && datums.get(datumKey).timestamp.get() >= timestamp)) {
                        // # 把key加入set, 用set保存key, 后续批量处理
                        batch.add(datumKey);
                    }
                    // # 如果set内的数据太少, 处理的数量 小于 远端发来的心跳datum数量 直接返回,等数量够了批量处理
                    if (batch.size() < 50 && processedCount < beatDatums.size()) {
                        continue;
                    }

                    String keys = StringUtils.join(batch, ",");

                    if (batch.size() <= 0) {
                        continue;
                    }

                    Loggers.RAFT.info("get datums from leader: {}, batch size is {}, processedCount is {}"
                                    + ", datums' size is {}, RaftCore.datums' size is {}", getLeader().ip, batch.size(),
                            processedCount, beatDatums.size(), datums.size());

                    // update datum entry
                    // # 构建url v1/ns/raft/datum remote=远端发来的leader
                    String url = buildUrl(remote.ip, API_GET);
                    Map<String, String> queryParam = new HashMap<>(1);
                    queryParam.put("keys", URLEncoder.encode(keys, "UTF-8"));
                    // # 用最大的key, 发回给leader
                    HttpClient.asyncHttpGet(url, null, queryParam, new Callback<String>() {
                        // ! 回调
                        @Override
                        public void onReceive(RestResult<String> result) {
                            if (!result.ok()) {
                                return;
                            }

                            List<JsonNode> datumList = JacksonUtils
                                    .toObj(result.getData(), new TypeReference<List<JsonNode>>() {
                                    });

                            for (JsonNode datumJson : datumList) {
                                Datum newDatum = null;
                                OPERATE_LOCK.lock();
                                try {

                                    Datum oldDatum = getDatum(datumJson.get("key").asText());

                                    if (oldDatum != null && datumJson.get("timestamp").asLong() <= oldDatum.timestamp
                                            .get()) {
                                        Loggers.RAFT
                                                .info("[NACOS-RAFT] timestamp is smaller than that of mine, key: {}, remote: {}, local: {}",
                                                        datumJson.get("key").asText(),
                                                        datumJson.get("timestamp").asLong(), oldDatum.timestamp);
                                        continue;
                                    }

                                    if (KeyBuilder.matchServiceMetaKey(datumJson.get("key").asText())) {
                                        Datum<Service> serviceDatum = new Datum<>();
                                        serviceDatum.key = datumJson.get("key").asText();
                                        serviceDatum.timestamp.set(datumJson.get("timestamp").asLong());
                                        serviceDatum.value = JacksonUtils
                                                .toObj(datumJson.get("value").toString(), Service.class);
                                        newDatum = serviceDatum;
                                    }

                                    if (KeyBuilder.matchInstanceListKey(datumJson.get("key").asText())) {
                                        Datum<Instances> instancesDatum = new Datum<>();
                                        instancesDatum.key = datumJson.get("key").asText();
                                        instancesDatum.timestamp.set(datumJson.get("timestamp").asLong());
                                        instancesDatum.value = JacksonUtils
                                                .toObj(datumJson.get("value").toString(), Instances.class);
                                        newDatum = instancesDatum;
                                    }

                                    if (newDatum == null || newDatum.value == null) {
                                        Loggers.RAFT.error("receive null datum: {}", datumJson);
                                        continue;
                                    }
                                    // # 写文件
                                    raftStore.write(newDatum);
                                    // # 写入内存
                                    datums.put(newDatum.key, newDatum);
                                    //  # 通知, 发布事件
                                    notifier.notify(newDatum.key, DataOperation.CHANGE, newDatum.value);

                                    local.resetLeaderDue();

                                    if (local.term.get() + 100 > remote.term.get()) {
                                        getLeader().term.set(remote.term.get());
                                        local.term.set(getLeader().term.get());
                                    } else {
                                        local.term.addAndGet(100);
                                    }

                                    raftStore.updateTerm(local.term.get());

                                    Loggers.RAFT.info("data updated, key: {}, timestamp: {}, from {}, local term: {}",
                                            newDatum.key, newDatum.timestamp, JacksonUtils.toJson(remote), local.term);

                                } catch (Throwable e) {
                                    Loggers.RAFT
                                            .error("[RAFT-BEAT] failed to sync datum from leader, datum: {}", newDatum,
                                                    e);
                                } finally {
                                    OPERATE_LOCK.unlock();
                                }
                            }
                            try {
                                TimeUnit.MILLISECONDS.sleep(200);
                            } catch (InterruptedException e) {
                                Loggers.RAFT.error("[RAFT-BEAT] Interrupted error ", e);
                            }
                            return;
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            Loggers.RAFT.error("[RAFT-BEAT] failed to sync datum from leader", throwable);
                        }

                        @Override
                        public void onCancel() {

                        }

                    });

                    batch.clear();

                } catch (Exception e) {
                    Loggers.RAFT.error("[NACOS-RAFT] failed to handle beat entry, key: {}", datumKey);
                }

            }

            List<String> deadKeys = new ArrayList<>();
            // # 遍历map, value为0, value=0是本地数据,如果是1是远端数据, 经过前面处理后,这时候value=0的数据,说明key已经被leader删掉了
            // # 把需要删除的key放入List
            for (Map.Entry<String, Integer> entry : receivedKeysMap.entrySet()) {
                if (entry.getValue() == 0) {
                    deadKeys.add(entry.getKey());
                }
            }

            // # 遍历需要删除的key, 执行删除本地datum
            for (String deadKey : deadKeys) {
                try {
                    // ! 从本地Datum文件中,删除掉这个key, 与leader保持一致
                    deleteDatum(deadKey);
                } catch (Exception e) {
                    Loggers.RAFT.error("[NACOS-RAFT] failed to remove entry, key={} {}", deadKey, e);
                }
            }

        }

        return local;
    }

    /**
     * Add listener for target key.
     *
     * @param key      key
     * @param listener new listener
     */
    public void listen(String key, RecordListener listener) {
        notifier.registerListener(key, listener);

        Loggers.RAFT.info("add listener: {}", key);
        // if data present, notify immediately
        for (Datum datum : datums.values()) {
            if (!listener.interests(datum.key)) {
                continue;
            }

            try {
                listener.onChange(datum.key, datum.value);
            } catch (Exception e) {
                Loggers.RAFT.error("NACOS-RAFT failed to notify listener", e);
            }
        }
    }

    /**
     * Remove listener for key.
     *
     * @param key      key
     * @param listener listener
     */
    public void unListen(String key, RecordListener listener) {
        notifier.deregisterListener(key, listener);
    }

    public void unListenAll(String key) {
        notifier.deregisterAllListener(key);
    }

    public void setTerm(long term) {
        peers.setTerm(term);
    }

    public boolean isLeader(String ip) {
        return peers.isLeader(ip);
    }

    public boolean isLeader() {
        return peers.isLeader(NetUtils.localServer());
    }

    /**
     * Build api url.
     *
     * @param ip  ip of api
     * @param api api path
     * @return api url
     */
    public static String buildUrl(String ip, String api) {
        if (!IPUtil.containsPort(ip)) {
            ip = ip + IPUtil.IP_PORT_SPLITER + EnvUtil.getPort();
        }
        return "http://" + ip + EnvUtil.getContextPath() + api;
    }

    public Datum<?> getDatum(String key) {
        return datums.get(key);
    }

    public RaftPeer getLeader() {
        return peers.getLeader();
    }

    public List<RaftPeer> getPeers() {
        return new ArrayList<>(peers.allPeers());
    }

    public RaftPeerSet getPeerSet() {
        return peers;
    }

    public void setPeerSet(RaftPeerSet peerSet) {
        peers = peerSet;
    }

    public int datumSize() {
        return datums.size();
    }

    public void addDatum(Datum datum) {
        datums.put(datum.key, datum);
        NotifyCenter.publishEvent(ValueChangeEvent.builder().key(datum.key).action(DataOperation.CHANGE).build());
    }

    /**
     * Load datum.
     *
     * @param key datum key
     */
    public void loadDatum(String key) {
        try {
            Datum datum = raftStore.load(key);
            if (datum == null) {
                return;
            }
            datums.put(key, datum);
        } catch (Exception e) {
            Loggers.RAFT.error("load datum failed: " + key, e);
        }

    }
    // ! 从本地Datum文件中,删除掉这个key, 与leader保持一致
    private void deleteDatum(String key) {
        Datum deleted;
        try {
            deleted = datums.remove(URLDecoder.decode(key, "UTF-8"));
            if (deleted != null) {
                raftStore.delete(deleted);
                Loggers.RAFT.info("datum deleted, key: {}", key);
            }
            NotifyCenter.publishEvent(
                    ValueChangeEvent.builder().key(URLDecoder.decode(key, "UTF-8")).action(DataOperation.DELETE)
                            .build());
        } catch (UnsupportedEncodingException e) {
            Loggers.RAFT.warn("datum key decode failed: {}", key);
        }
    }

    public boolean isInitialized() {
        return initialized || !globalConfig.isDataWarmup();
    }

    @Deprecated
    public int getNotifyTaskCount() {
        return (int) publisher.currentEventSize();
    }

}
