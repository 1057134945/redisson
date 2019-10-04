/**
 * Copyright (c) 2013-2019 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.RedisException;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommand.ValueType;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.client.protocol.convertor.IntegerReplayConvertor;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.connection.ConnectionManager;
import org.redisson.misc.RPromise;
import org.redisson.misc.RedissonPromise;
import org.redisson.pubsub.LockPubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;

/**
 * Distributed implementation of {@link java.util.concurrent.locks.Lock}
 * Implements reentrant lock.<br>
 * Lock will be removed automatically if client disconnects.
 * <p>
 * Implements a <b>non-fair</b> locking so doesn't guarantees an acquire order.
 *
 * @author Nikita Koksharov
 */
public class RedissonLock extends RedissonExpirable implements RLock {

    public static class ExpirationEntry {

        /**
         * 线程与计数器的映射
         *
         * KEY：线程编号
         * VALUE：计数
         */
        private final Map<Long, Integer> threadIds = new LinkedHashMap<>();
        /**
         * 定时任务
         */
        private volatile Timeout timeout;

        public ExpirationEntry() {
            super();
        }

        /**
         * 增加线程的计数
         *
         * @param threadId 线程编号
         */
        public void addThreadId(long threadId) {
            Integer counter = threadIds.get(threadId);
            if (counter == null) {
                counter = 1;
            } else {
                counter++;
            }
            threadIds.put(threadId, counter);
        }

        public boolean hasNoThreads() {
            return threadIds.isEmpty();
        }

        public Long getFirstThreadId() {
            if (threadIds.isEmpty()) {
                return null;
            }
            return threadIds.keySet().iterator().next();
        }

        /**
         * 减少线程的技术
         *
         * @param threadId 线程编号
         */
        public void removeThreadId(long threadId) {
            Integer counter = threadIds.get(threadId);
            if (counter == null) {
                return;
            }
            counter--;
            if (counter == 0) {
                threadIds.remove(threadId);
            } else {
                threadIds.put(threadId, counter);
            }
        }

        public void setTimeout(Timeout timeout) {
            this.timeout = timeout;
        }

        public Timeout getTimeout() {
            return timeout;
        }

    }

    private static final Logger log = LoggerFactory.getLogger(RedissonLock.class);

    /**
     * ExpirationEntry 的映射
     *
     * key ：{@link #entryName}
     */
    private static final ConcurrentMap<String, ExpirationEntry> EXPIRATION_RENEWAL_MAP = new ConcurrentHashMap<>();

    /**
     * 锁的时长
     */
    protected long internalLockLeaseTime;

    /**
     * ID ，就是 {@link ConnectionManager#getId()}
     */
    final String id;
    /**
     * Sub Entry 名字
     */
    final String entryName;

    protected final LockPubSub pubSub;

    final CommandAsyncExecutor commandExecutor;

    public RedissonLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.id = commandExecutor.getConnectionManager().getId();
        this.internalLockLeaseTime = commandExecutor.getConnectionManager().getCfg().getLockWatchdogTimeout();
        this.entryName = id + ":" + name;
        this.pubSub = commandExecutor.getConnectionManager().getSubscribeService().getLockPubSub();
    }

    protected String getEntryName() {
        return entryName;
    }

    String getChannelName() {
        return prefixName("redisson_lock__channel", getName());
    }

    protected String getLockName(long threadId) {
        return id + ":" + threadId;
    }

    @Override
    public void lock() {
        try {
            lock(-1, null, false);
        } catch (InterruptedException e) {
            throw new IllegalStateException();
        }
    }

    @Override
    public void lock(long leaseTime, TimeUnit unit) {
        try {
            lock(leaseTime, unit, false);
        } catch (InterruptedException e) {
            throw new IllegalStateException();
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        lock(-1, null, true);
    }

    @Override
    public void lockInterruptibly(long leaseTime, TimeUnit unit) throws InterruptedException {
        lock(leaseTime, unit, true);
    }

    private void lock(long leaseTime, TimeUnit unit, boolean interruptibly) throws InterruptedException {
        long threadId = Thread.currentThread().getId();
        // 同步获加锁
        Long ttl = tryAcquire(leaseTime, unit, threadId);
        // lock acquired
        // 加锁成功，直接返回
        if (ttl == null) {
            return;
        }

        // 创建 SUBSCRIBE 订阅的 Future
        RFuture<RedissonLockEntry> future = subscribe(threadId);
        // 阻塞等待订阅发起成功
        commandExecutor.syncSubscription(future);

        try {
            while (true) {
                // 同步获加锁
                ttl = tryAcquire(leaseTime, unit, threadId);
                // lock acquired
                // 加锁成功，直接返回
                if (ttl == null) {
                    break;
                }

                // waiting for message
                // 通过 RedissonLockEntry 的信号量，阻塞等待锁的释放消息，或者 ttl/time 超时（例如说，锁的自动超时释放）
                if (ttl >= 0) {
                    try {
                        getEntry(threadId).getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        // 如果允许打断，则抛出 e
                        if (interruptibly) {
                            throw e;
                        }
                        // 如果不允许打断，则继续
                        getEntry(threadId).getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                    }
                } else {
                    if (interruptibly) {
                        getEntry(threadId).getLatch().acquire();
                    } else {
                        getEntry(threadId).getLatch().acquireUninterruptibly();
                    }
                }
            }
        } finally {
            // 小细节，需要最终取消 SUBSCRIBE 订阅
            unsubscribe(future, threadId);
        }
//        get(lockAsync(leaseTime, unit));
    }

    private Long tryAcquire(long leaseTime, TimeUnit unit, long threadId) {
        return get(tryAcquireAsync(leaseTime, unit, threadId));
    }

    private RFuture<Boolean> tryAcquireOnceAsync(long leaseTime, TimeUnit unit, long threadId) {
        // 情况一，如果锁有时长，则直接获得分布式锁
        if (leaseTime != -1) {
            return tryLockInnerAsync(leaseTime, unit, threadId, RedisCommands.EVAL_NULL_BOOLEAN);
        }

        // 情况二，如果锁无时长，则先获得 Lock WatchDog 的锁超时时长
        RFuture<Boolean> ttlRemainingFuture = tryLockInnerAsync(commandExecutor.getConnectionManager().getCfg().getLockWatchdogTimeout(), TimeUnit.MILLISECONDS, threadId, RedisCommands.EVAL_NULL_BOOLEAN);
        ttlRemainingFuture.onComplete((ttlRemaining, e) -> {
            // 如果发生异常，则直接返回
            if (e != null) {
                return;
            }

            // lock acquired
            // 如果获得到锁，则创建定时任务，定时续锁
            if (ttlRemaining) {
                scheduleExpirationRenewal(threadId);
            }
        });
        return ttlRemainingFuture;
    }

    private <T> RFuture<Long> tryAcquireAsync(long leaseTime, TimeUnit unit, long threadId) {
        // 情况一，如果锁有时长，则直接获得分布式锁
        if (leaseTime != -1) {
            return tryLockInnerAsync(leaseTime, unit, threadId, RedisCommands.EVAL_LONG);
        }

        // 情况二，如果锁无时长，则先获得 Lock WatchDog 的锁超时时长
        RFuture<Long> ttlRemainingFuture = tryLockInnerAsync(commandExecutor.getConnectionManager().getCfg().getLockWatchdogTimeout(), TimeUnit.MILLISECONDS, threadId, RedisCommands.EVAL_LONG);
        ttlRemainingFuture.onComplete((ttlRemaining, e) -> {
            // 如果发生异常，则直接返回
            if (e != null) {
                return;
            }

            // lock acquired
            // 如果获得到锁，则创建定时任务，定时续锁
            if (ttlRemaining == null) {
                scheduleExpirationRenewal(threadId);
            }
        });
        return ttlRemainingFuture;
    }

    @Override
    public boolean tryLock() {
        return get(tryLockAsync());
    }

    private void renewExpiration() {
        // 获得 ExpirationEntry 队形，从 EXPIRATION_RENEWAL_MAP 中
        ExpirationEntry ee = EXPIRATION_RENEWAL_MAP.get(getEntryName());
        if (ee == null) { // 如果不存在，返回
            return;
        }

        // 创建 Timeout 定时任务，实现定时续锁
        Timeout task = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {

            @Override
            public void run(Timeout timeout) throws Exception {
                // 获得 ExpirationEntry 对象
                ExpirationEntry ent = EXPIRATION_RENEWAL_MAP.get(getEntryName());
                if (ent == null) { // 如果不存在，返回
                    return;
                }
                // 获得 threadId 编号
                Long threadId = ent.getFirstThreadId();
                if (threadId == null) { // 如果不存在，则返回
                    return;
                }

                // 执行续锁
                RFuture<Boolean> future = renewExpirationAsync(threadId);
                future.onComplete((res, e) -> {
                    // 如果发生异常，则打印异常日志，并返回。此时，就不会在定时续租了
                    if (e != null) {
                        log.error("Can't update lock " + getName() + " expiration", e);
                        return;
                    }

                    // 续锁成功，则重新发起定时任务
                    if (res) {
                        // reschedule itself
                        renewExpiration();
                    }
                });
            }

        }, internalLockLeaseTime / 3, TimeUnit.MILLISECONDS); // 定时，每 internalLockLeaseTime / 3 秒执行一次。

        // 设置定时任务到 ExpirationEntry 中
        ee.setTimeout(task);
    }

    private void scheduleExpirationRenewal(long threadId) {
        // 创建 ExpirationEntry 对象
        ExpirationEntry entry = new ExpirationEntry();
        // 添加到 EXPIRATION_RENEWAL_MAP 中
        ExpirationEntry oldEntry = EXPIRATION_RENEWAL_MAP.putIfAbsent(getEntryName(), entry);
        // 添加线程编号到 ExpirationEntry 中
        if (oldEntry != null) {
            oldEntry.addThreadId(threadId);
        } else {
            entry.addThreadId(threadId);
            // 创建定时任务，用于续锁
            renewExpiration();
        }
    }

    protected RFuture<Boolean> renewExpirationAsync(long threadId) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " + // 情况一，如果持有锁，则重新设置过期时间为 ARGV[1] internalLockLeaseTime ，并返回 1 续租成功。
                    "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                    "return 1; " +
                "end; " +
                "return 0;", // 情况二，未吃货有，返回 0 续租失败。
            Collections.<Object>singletonList(getName()),
            internalLockLeaseTime, getLockName(threadId));
    }

    void cancelExpirationRenewal(Long threadId) {
        // 获得 ExpirationEntry 对象
        ExpirationEntry task = EXPIRATION_RENEWAL_MAP.get(getEntryName());
        if (task == null) { // 如果不存在，返回
            return;
        }

        // 从 ExpirationEntry 中，移除线程编号
        if (threadId != null) {
            task.removeThreadId(threadId);
        }

        // 如果 ExpirationEntry 的所有线程被清空
        if (threadId == null || task.hasNoThreads()) {
            // 取消定时任务
            task.getTimeout().cancel();
            // 从 EXPIRATION_RENEWAL_MAP 中移除
            EXPIRATION_RENEWAL_MAP.remove(getEntryName());
        }
    }

    /**
     * 通过 Lua 脚本，实现获得分布式锁。
     *
     * @param leaseTime 锁的时长
     * @param unit 锁的时长单位
     * @param threadId 线程编号
     * @param command 指令
     * @param <T> 返回结果的泛型
     * @return 返回 RFuture 对象
     */
    <T> RFuture<T> tryLockInnerAsync(long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        internalLockLeaseTime = unit.toMillis(leaseTime);

        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, command,
                  "if (redis.call('exists', KEYS[1]) == 0) then " + // 情况一，当前分布式锁被未被获得
                      "redis.call('hset', KEYS[1], ARGV[2], 1); " + // 写入分布锁被 ARGV[2] 获取到了，设置数量为 1 。
                      "redis.call('pexpire', KEYS[1], ARGV[1]); " + // 设置分布式的过期时间为 ARGV[1]
                      "return nil; " + // 返回 null ，表示成功
                  "end; " +
                  "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " + // 情况二，如果当前分布锁已经被 ARGV[2] 持有
                      "redis.call('hincrby', KEYS[1], ARGV[2], 1); " + // 写入持有计数字 + 1 。
                      "redis.call('pexpire', KEYS[1], ARGV[1]); " + //  设置分布式的过期时间为 ARGV[1]
                      "return nil; " + // 返回 null ，表示成功
                  "end; " +
                  "return redis.call('pttl', KEYS[1]);", // 情况三，获取不到分布式锁，则返回锁的过期时间。
                    Collections.<Object>singletonList(getName()), // KEYS[分布式锁名]
                internalLockLeaseTime, getLockName(threadId)); // ARGV[锁超时时间，获得的锁名]
    }

    private void acquireFailed(long threadId) {
        get(acquireFailedAsync(threadId));
    }

    protected RFuture<Void> acquireFailedAsync(long threadId) {
        return RedissonPromise.newSucceededFuture(null);
    }

    @Override
    public boolean tryLock(long waitTime, long leaseTime, TimeUnit unit) throws InterruptedException {
        long time = unit.toMillis(waitTime);
        long current = System.currentTimeMillis();
        long threadId = Thread.currentThread().getId();
        // 同步获加锁
        Long ttl = tryAcquire(leaseTime, unit, threadId);
        // lock acquired
        // 加锁成功，直接返回 true 加锁成功
        if (ttl == null) {
            return true;
        }

        // 减掉已经等待的时间
        time -= System.currentTimeMillis() - current;
        // 如果无剩余等待的时间，则返回 false 加锁失败
        if (time <= 0) {
            acquireFailed(threadId);
            return false;
        }

        // 记录新的当前时间
        current = System.currentTimeMillis();
        // 创建 SUBSCRIBE 订阅的 Future
        RFuture<RedissonLockEntry> subscribeFuture = subscribe(threadId);
        // 阻塞等待订阅发起成功
        if (!await(subscribeFuture, time, TimeUnit.MILLISECONDS)) {
            // 进入到此处，说明阻塞等待发起订阅超时
            // 取消 SUBSCRIBE 订阅
            if (!subscribeFuture.cancel(false)) {
                // 进入到此处，说明取消发起订阅失败，则通过设置回调，在启发订阅完成后，回调取消 SUBSCRIBE 订阅
                subscribeFuture.onComplete((res, e) -> {
                    if (e == null) {
                        unsubscribe(subscribeFuture, threadId);
                    }
                });
            }
            // 等待超时，则返回 false 加锁失败
            acquireFailed(threadId);
            return false;
        }

        try {
            // 减掉已经等待的时间
            time -= System.currentTimeMillis() - current;
            // 如果无剩余等待的时间，则返回 false 加锁失败
            if (time <= 0) {
                acquireFailed(threadId);
                return false;
            }

            while (true) {
                // 记录新的当前时间
                long currentTime = System.currentTimeMillis();
                // 同步获加锁
                ttl = tryAcquire(leaseTime, unit, threadId);
                // lock acquired
                // 加锁成功，直接返回 true 加锁成功
                if (ttl == null) {
                    return true;
                }

                // 减掉已经等待的时间
                time -= System.currentTimeMillis() - currentTime;
                // 如果无剩余等待的时间，则返回 false 加锁失败
                if (time <= 0) {
                    acquireFailed(threadId);
                    return false;
                }

                // waiting for message
                // 记录新的当前时间
                currentTime = System.currentTimeMillis();

                // 通过 RedissonLockEntry 的信号量，阻塞等待锁的释放消息，或者 ttl/time 超时（例如说，锁的自动超时释放）
                if (ttl >= 0 && ttl < time) {
                    getEntry(threadId).getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                } else {
                    getEntry(threadId).getLatch().tryAcquire(time, TimeUnit.MILLISECONDS);
                }

                // 减掉已经等待的时间
                time -= System.currentTimeMillis() - currentTime;
                // 如果无剩余等待的时间，则返回 false 加锁失败
                if (time <= 0) {
                    acquireFailed(threadId);
                    return false;
                }
            }
        } finally {
            // 小细节，需要最终取消 SUBSCRIBE 订阅
            unsubscribe(subscribeFuture, threadId);
        }
//        return get(tryLockAsync(waitTime, leaseTime, unit));
    }

    /**
     * 获得线程对应的 RedissonLockEntry 对象
     *
     * @param threadId 线程编号
     * @return RedissonLockEntry 对象
     */
    protected RedissonLockEntry getEntry(long threadId) {
        return pubSub.getEntry(getEntryName());
    }

    /**
     * 异步发起订阅
     *
     * @param threadId 线程编号
     * @return RFuture 对象
     */
    protected RFuture<RedissonLockEntry> subscribe(long threadId) {
        return pubSub.subscribe(getEntryName(), getChannelName());
    }

    /**
     * 异步取消订阅
     *
     * @param future RFuture 对象
     * @param threadId 线程编号
     */
    protected void unsubscribe(RFuture<RedissonLockEntry> future, long threadId) {
        pubSub.unsubscribe(future.getNow(), getEntryName(), getChannelName());
    }

    @Override
    public boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException {
        return tryLock(waitTime, -1, unit);
    }

    @Override
    public void unlock() {
        try {
            get(unlockAsync(Thread.currentThread().getId()));
        } catch (RedisException e) {
            if (e.getCause() instanceof IllegalMonitorStateException) {
                throw (IllegalMonitorStateException) e.getCause();
            } else {
                throw e;
            }
        }

//        Future<Void> future = unlockAsync();
//        future.awaitUninterruptibly();
//        if (future.isSuccess()) {
//            return;
//        }
//        if (future.cause() instanceof IllegalMonitorStateException) {
//            throw (IllegalMonitorStateException)future.cause();
//        }
//        throw commandExecutor.convertException(future);
    }

    @Override
    public Condition newCondition() {
        // TODO implement
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean forceUnlock() {
        return get(forceUnlockAsync());
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('del', KEYS[1]) == 1) then " // 情况一，释放锁成功，则通过 Publish 发布释放锁的消息，并返回 1
                + "redis.call('publish', KEYS[2], ARGV[1]); "
                + "return 1 "
                + "else " // 情况二，释放锁失败，因为不存在这个 KEY ，所以返回 0
                + "return 0 "
                + "end",
                Arrays.<Object>asList(getName(), getChannelName()),
                LockPubSub.UNLOCK_MESSAGE);
    }

    @Override
    public boolean isLocked() {
        return isExists();
    }

    @Override
    public RFuture<Boolean> isLockedAsync() {
        return isExistsAsync();
    }

    @Override
    public RFuture<Boolean> isExistsAsync() {
        return commandExecutor.writeAsync(getName(), codec, RedisCommands.EXISTS, getName());
    }

    @Override
    public boolean isHeldByCurrentThread() {
        return isHeldByThread(Thread.currentThread().getId());
    }

    @Override
    public boolean isHeldByThread(long threadId) {
        RFuture<Boolean> future = commandExecutor.writeAsync(getName(), LongCodec.INSTANCE, RedisCommands.HEXISTS, getName(), getLockName(threadId));
        return get(future);
    }

    private static final RedisCommand<Integer> HGET = new RedisCommand<Integer>("HGET", ValueType.MAP_VALUE, new IntegerReplayConvertor(0));

    public RFuture<Integer> getHoldCountAsync() {
        return commandExecutor.writeAsync(getName(), LongCodec.INSTANCE, HGET, getName(), getLockName(Thread.currentThread().getId()));
    }

    @Override
    public int getHoldCount() {
        return get(getHoldCountAsync());
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        return forceUnlockAsync();
    }

    @Override
    public RFuture<Void> unlockAsync() {
        // 获得线程编号
        long threadId = Thread.currentThread().getId();
        // 执行解锁
        return unlockAsync(threadId);
    }

    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " + // 情况一，分布式锁未被 ARGV[3] 持有，则直接返回 null ，表示解锁失败。
                    "return nil;" +
                "end; " +
                "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " + // 持有锁的数量减 1 。
                "if (counter > 0) then " + // 情况二，如果后还有剩余的持有锁数量，则返回 0 ，表示解锁未完成
                    "redis.call('pexpire', KEYS[1], ARGV[2]); " + // 重新设置过期时间为 ARGV[2]
                    "return 0; " +
                "else " + // 情况三，不存在剩余的锁数量，则返回 1 ，表示解锁成功
                    "redis.call('del', KEYS[1]); " + // 删除对应的分布式锁对应的 KEYS[1]
                    "redis.call('publish', KEYS[2], ARGV[1]); " + // 发布解锁事件到 KEYS[2] ，通知气他可能要获取锁的线程
                    "return 1; "+
                "end; " +
                "return nil;", // 不存在这个情况。
                Arrays.<Object>asList(getName(), getChannelName()), // KEYS[分布式锁名, 该分布式锁对应的 Channel 名]
                LockPubSub.UNLOCK_MESSAGE, internalLockLeaseTime, getLockName(threadId)); // ARGV[解锁消息，锁超时时间，获得的锁名]
    }

    @Override
    public RFuture<Void> unlockAsync(long threadId) {
        // 创建 RPromise 对象，用于异步回调
        RPromise<Void> result = new RedissonPromise<Void>();

        // 解锁逻辑
        RFuture<Boolean> future = unlockInnerAsync(threadId);

        future.onComplete((opStatus, e) -> {
            // 如果发生异常，并通过 result 通知异常
            if (e != null) {
                cancelExpirationRenewal(threadId);
                result.tryFailure(e);
                return;
            }

            // 解锁的线程不对，则创建 IllegalMonitorStateException 异常，并通过 result 通知异常
            if (opStatus == null) {
                IllegalMonitorStateException cause = new IllegalMonitorStateException("attempt to unlock lock, not locked by current thread by node id: "
                        + id + " thread-id: " + threadId);
                result.tryFailure(cause);
                return;
            }

            // 取消定时过期
            cancelExpirationRenewal(threadId);

            // 通知 result 解锁成功
            result.trySuccess(null);
        });

        return result;
    }

    @Override
    public RFuture<Void> lockAsync() {
        return lockAsync(-1, null);
    }

    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit) {
        // 获得线程编号
        long currentThreadId = Thread.currentThread().getId();
        // 异步锁
        return lockAsync(leaseTime, unit, currentThreadId);
    }

    @Override
    public RFuture<Void> lockAsync(long currentThreadId) {
        return lockAsync(-1, null, currentThreadId);
    }

    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit, long currentThreadId) {
        // 创建 RPromise 对象，用于异步回调
        RPromise<Void> result = new RedissonPromise<Void>();
        // 异步加锁
        RFuture<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, currentThreadId);
        ttlFuture.onComplete((ttl, e) -> {
            // 如果发生异常，则通过 result 通知异常
            if (e != null) {
                result.tryFailure(e);
                return;
            }

            // lock acquired
            // 如果获得到锁，则通过 result 通知获得锁成功
            if (ttl == null) {
                if (!result.trySuccess(null)) { // 如果处理 result 通知对结果返回 false ，意味着需要异常释放锁
                    unlockAsync(currentThreadId);
                }
                return;
            }

            // 创建 SUBSCRIBE 订阅的 Future
            RFuture<RedissonLockEntry> subscribeFuture = subscribe(currentThreadId);
            subscribeFuture.onComplete((res, ex) -> {
                // 如果发生异常，则通过 result 通知异常
                if (ex != null) {
                    result.tryFailure(ex);
                    return;
                }

                // 异步加锁
                lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
            });
        });

        return result;
    }

    private void lockAsync(long leaseTime, TimeUnit unit, RFuture<RedissonLockEntry> subscribeFuture, RPromise<Void> result, long currentThreadId) {
        // 获得分布式锁
        RFuture<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, currentThreadId);
        ttlFuture.onComplete((ttl, e) -> {
            // 如果发生异常，则取消订阅，并通过 result 通知异常
            if (e != null) {
                unsubscribe(subscribeFuture, currentThreadId);
                result.tryFailure(e);
                return;
            }

            // lock acquired
            // 如果获得到锁，则取消订阅，并通过 result 通知获得锁成功
            if (ttl == null) {
                unsubscribe(subscribeFuture, currentThreadId);
                if (!result.trySuccess(null)) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            // 获得当前线程对应的 RedissonLockEntry 对象
            RedissonLockEntry entry = getEntry(currentThreadId);
            // 尝试获得 entry 中的信号量，如果获得成功，说明 SUBSCRIBE 已经收到释放锁的消息，则直接立马再次去获得锁。
            if (entry.getLatch().tryAcquire()) {
                lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
            } else {
                // waiting for message
                // 创建 AtomicReference 对象，用于指向定时任务
                AtomicReference<Timeout> futureRef = new AtomicReference<Timeout>();
                // 创建监听器 listener ，用于在 RedissonLockEntry 的回调，就是我们看到的 PublishSubscribe 监听到释放锁的消息，进行回调。
                Runnable listener = () -> {
                    // 如果有定时任务的 Future ，则进行取消
                    if (futureRef.get() != null) {
                        futureRef.get().cancel();
                    }
                    // 再次获得分布式锁
                    lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
                };
                // 添加 listener 到 RedissonLockEntry 中
                entry.addListener(listener);

                // 下面，会创建一个定时任务。因为极端情况下，可能不存在释放锁的消息，例如说锁自动超时释放，所以需要改定时任务，在获得到锁的超时后，主动去抢下。
                if (ttl >= 0) {
                    Timeout scheduledFuture = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
                        @Override
                        public void run(Timeout timeout) throws Exception {
                            // 移除 listener 从 RedissonLockEntry 中
                            if (entry.removeListener(listener)) {
                                // 再次获得分布式锁
                                lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
                            }
                        }
                    }, ttl, TimeUnit.MILLISECONDS);
                    // 记录 futureRef 执行 scheduledFuture
                    futureRef.set(scheduledFuture);
                }
            }
        });
    }

    @Override
    public RFuture<Boolean> tryLockAsync() {
        return tryLockAsync(Thread.currentThread().getId());
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long threadId) {
        return tryAcquireOnceAsync(-1, null, threadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, TimeUnit unit) {
        return tryLockAsync(waitTime, -1, unit);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit) {
        // 获得线程编号
        long currentThreadId = Thread.currentThread().getId();
        // 执行锁
        return tryLockAsync(waitTime, leaseTime, unit, currentThreadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit, long currentThreadId) {
        // 创建 RPromise 对象，用于通知结果
        RPromise<Boolean> result = new RedissonPromise<Boolean>();

        // 表示剩余的等待获得锁的时间
        AtomicLong time = new AtomicLong(unit.toMillis(waitTime));
        // 记录当前时间
        long currentTime = System.currentTimeMillis();
        // 执行异步获得锁
        RFuture<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, currentThreadId);
        ttlFuture.onComplete((ttl, e) -> {
            // 如果发生异常，则通过 result 通知异常
            if (e != null) {
                result.tryFailure(e);
                return;
            }

            // lock acquired
            // 如果获得到锁，则通过 result 通知获得锁成功
            if (ttl == null) {
                if (!result.trySuccess(true)) { // 如果处理 result 通知对结果返回 false ，意味着需要异常释放锁
                    unlockAsync(currentThreadId);
                }
                return;
            }

            // 减掉已经等待的时间
            long el = System.currentTimeMillis() - currentTime;
            time.addAndGet(-el);

            // 如果无剩余等待的时间，则通过 result 通知获得锁失败
            if (time.get() <= 0) {
                trySuccessFalse(currentThreadId, result);
                return;
            }

            // 记录新的当前时间
            long current = System.currentTimeMillis();
            // 记录下面的 future 的指向
            AtomicReference<Timeout> futureRef = new AtomicReference<Timeout>();

            // 创建 SUBSCRIBE 订阅的 Future
            RFuture<RedissonLockEntry> subscribeFuture = subscribe(currentThreadId);
            subscribeFuture.onComplete((r, ex) -> {
                // 如果发生异常，则通过 result 通知异常
                if (ex != null) {
                    result.tryFailure(ex);
                    return;
                }

                // 如果创建定时任务 Future scheduledFuture，则进行取消
                if (futureRef.get() != null) {
                    futureRef.get().cancel();
                }

                // 减掉已经等待的时间
                long elapsed = System.currentTimeMillis() - current;
                time.addAndGet(-elapsed);

                // 再次执行异步获得锁
                tryLockAsync(time, leaseTime, unit, subscribeFuture, result, currentThreadId);
            });

            // 如果创建 SUBSCRIBE 订阅的 Future 未完成，创建定时任务 Future scheduledFuture 。
            if (!subscribeFuture.isDone()) {
                Timeout scheduledFuture = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
                    @Override
                    public void run(Timeout timeout) throws Exception {
                        // 如果创建 SUBSCRIBE 订阅的 Future 未完成
                        if (!subscribeFuture.isDone()) {
                            // 进行取消 subscribeFuture
                            subscribeFuture.cancel(false);
                            // 通过 result 通知获得锁失败
                            trySuccessFalse(currentThreadId, result);
                        }
                    }
                }, time.get(), TimeUnit.MILLISECONDS); // 延迟 time 秒后执行
                // 记录 futureRef 执行 scheduledFuture
                futureRef.set(scheduledFuture);
            }
        });

        return result;
    }

    private void trySuccessFalse(long currentThreadId, RPromise<Boolean> result) {
        acquireFailedAsync(currentThreadId).onComplete((res, e) -> {
            if (e == null) { // 通知获得锁失败
                result.trySuccess(false);
            } else { // 通知异常
                result.tryFailure(e);
            }
        });
    }

    private void tryLockAsync(AtomicLong time, long leaseTime, TimeUnit unit, RFuture<RedissonLockEntry> subscribeFuture, RPromise<Boolean> result, long currentThreadId) {
        // 如果 result 已经完成，则直接返回，并取消订阅
        if (result.isDone()) {
            unsubscribe(subscribeFuture, currentThreadId);
            return;
        }

        // 如果剩余时间 time 小于 0 ，说明等待超时，则取消订阅，并通过 result 通知失败
        if (time.get() <= 0) {
            unsubscribe(subscribeFuture, currentThreadId);
            trySuccessFalse(currentThreadId, result);
            return;
        }

        // 记录当前时间
        long curr = System.currentTimeMillis();
        // 获得分布式锁
        RFuture<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, currentThreadId);
        ttlFuture.onComplete((ttl, e) -> {
                // 如果发生异常，则取消订阅，并通过 result 通知异常
                if (e != null) {
                    unsubscribe(subscribeFuture, currentThreadId);
                    result.tryFailure(e);
                    return;
                }

                // lock acquired
                // 如果获得到锁，则取消订阅，并通过 result 通知获得锁成功
                if (ttl == null) {
                    unsubscribe(subscribeFuture, currentThreadId);
                    if (!result.trySuccess(true)) {
                        unlockAsync(currentThreadId);
                    }
                    return;
                }

                // 减掉已经等待的时间
                long el = System.currentTimeMillis() - curr;
                time.addAndGet(-el);

                // 如果无剩余等待的时间，则取消订阅，并通过 result 通知获得锁失败
                if (time.get() <= 0) {
                    unsubscribe(subscribeFuture, currentThreadId);
                    trySuccessFalse(currentThreadId, result);
                    return;
                }

                // waiting for message
                // 记录新的当前时间
                long current = System.currentTimeMillis();
                // 获得当前线程对应的 RedissonLockEntry 对象
                RedissonLockEntry entry = getEntry(currentThreadId);
                // 尝试获得 entry 中的信号量，如果获得成功，说明 SUBSCRIBE 已经收到释放锁的消息，则直接立马再次去获得锁。
                if (entry.getLatch().tryAcquire()) {
                    tryLockAsync(time, leaseTime, unit, subscribeFuture, result, currentThreadId);
                } else {
                    // 创建 AtomicBoolean 变量 executed ，用于标记下面创建的 listener 是否执行。
                    AtomicBoolean executed = new AtomicBoolean();
                    // 创建 AtomicReference 对象，用于指向定时任务
                    AtomicReference<Timeout> futureRef = new AtomicReference<Timeout>();

                    // 创建监听器 listener ，用于在 RedissonLockEntry 的回调，就是我们看到的 PublishSubscribe 监听到释放锁的消息，进行回调。
                    Runnable listener = () -> {
                        // 标记已经执行
                        executed.set(true);
                        // 如果有定时任务的 Future ，则进行取消
                        if (futureRef.get() != null) {
                            futureRef.get().cancel();
                        }

                        // 减掉已经等待的时间
                        long elapsed = System.currentTimeMillis() - current;
                        time.addAndGet(-elapsed);

                        // 再次获得分布式锁
                        tryLockAsync(time, leaseTime, unit, subscribeFuture, result, currentThreadId);
                    };
                    // 添加 listener 到 RedissonLockEntry 中
                    entry.addListener(listener);

                    // 下面，会创建一个定时任务。因为极端情况下，可能不存在释放锁的消息，例如说锁自动超时释放，所以需要改定时任务，在获得到锁的超时后，主动去抢下。
                    long t = time.get();
                    if (ttl >= 0 && ttl < time.get()) { // 如果剩余时间小于锁的超时时间，则使用剩余时间。
                        t = ttl;
                    }
                    // 如果 listener 未执行
                    if (!executed.get()) {
                        Timeout scheduledFuture = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
                            @Override
                            public void run(Timeout timeout) throws Exception {
                                // 移除 listener 从 RedissonLockEntry 中
                                if (entry.removeListener(listener)) {
                                    // 减掉已经等待的时间
                                    long elapsed = System.currentTimeMillis() - current;
                                    time.addAndGet(-elapsed);

                                    // 再次获得分布式锁
                                    tryLockAsync(time, leaseTime, unit, subscribeFuture, result, currentThreadId);
                                }
                            }
                        }, t, TimeUnit.MILLISECONDS);
                        // 记录 futureRef 执行 scheduledFuture
                        futureRef.set(scheduledFuture);
                    }
                }
        });
    }


}
