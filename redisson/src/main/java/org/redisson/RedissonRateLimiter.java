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

import org.redisson.api.*;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommand.ValueType;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.decoder.MultiDecoder;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.misc.RPromise;
import org.redisson.misc.RedissonPromise;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 基于滑动窗口实现的 RateLimiter 实现
 *
 * @author Nikita Koksharov
 */
public class RedissonRateLimiter extends RedissonObject implements RRateLimiter {

    public RedissonRateLimiter(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
    }

    String getValueName() {
        return suffixName(getName(), "value");
    }

    String getClientValueName() {
        return suffixName(getValueName(), commandExecutor.getConnectionManager().getId());
    }

    @Override
    public boolean tryAcquire() {
        return tryAcquire(1);
    }

    @Override
    public RFuture<Boolean> tryAcquireAsync() {
        return tryAcquireAsync(1L);
    }

    @Override
    public boolean tryAcquire(long permits) {
        return get(tryAcquireAsync(RedisCommands.EVAL_NULL_BOOLEAN, permits));
    }

    @Override
    public RFuture<Boolean> tryAcquireAsync(long permits) {
        return tryAcquireAsync(RedisCommands.EVAL_NULL_BOOLEAN, permits);
    }

    @Override
    public void acquire() {
        get(acquireAsync());
    }

    @Override
    public RFuture<Void> acquireAsync() {
        return acquireAsync(1);
    }

    @Override
    public void acquire(long permits) {
        get(acquireAsync(permits));
    }

    @Override
    public RFuture<Void> acquireAsync(long permits) {
        // 创建 RPromise 对象
        RPromise<Void> promise = new RedissonPromise<Void>();
        // 执行获得令牌。通过 -1 ，表示重试到成功为止
        tryAcquireAsync(permits, -1, null).onComplete((res, e) -> {
            // 处理异常
            if (e != null) {
                promise.tryFailure(e);
                return;
            }

            // 成功
            promise.trySuccess(null);
        });
        return promise;
    }

    @Override
    public boolean tryAcquire(long timeout, TimeUnit unit) {
        return get(tryAcquireAsync(timeout, unit));
    }

    @Override
    public RFuture<Boolean> tryAcquireAsync(long timeout, TimeUnit unit) {
        return tryAcquireAsync(1, timeout, unit);
    }

    @Override
    public boolean tryAcquire(long permits, long timeout, TimeUnit unit) {
        return get(tryAcquireAsync(permits, timeout, unit));
    }

    @Override
    public RFuture<Boolean> tryAcquireAsync(long permits, long timeout, TimeUnit unit) {
        // 创建 RPromise 对象
        RPromise<Boolean> promise = new RedissonPromise<Boolean>();
        // 计算 timeoutInMillis
        long timeoutInMillis = -1;
        if (timeout >= 0) {
            timeoutInMillis = unit.toMillis(timeout);
        }
        // 执行获取令牌
        tryAcquireAsync(permits, promise, timeoutInMillis);
        return promise;
    }

    private void tryAcquireAsync(long permits, RPromise<Boolean> promise, long timeoutInMillis) {
        // 获得当前时间
        long s = System.currentTimeMillis();
        // 执行获得令牌
        RFuture<Long> future = tryAcquireAsync(RedisCommands.EVAL_LONG, permits);
        // 通过 future 回调处理执行结果
        future.onComplete((delay, e) -> {
            // 发生异常，则 return 并通过 promise 处理异常
            if (e != null) {
                promise.tryFailure(e);
                return;
            }

            // 如果未返回 delay ，而是空，说明获取锁成功了，则 return 并通过 promise 返回获得锁成功。
            if (delay == null) {
                promise.trySuccess(true);
                return;
            }

            // 如果 timeoutInMillis 为 -1 ，表示持续获得到锁，直到成功。
            if (timeoutInMillis == -1) {
                // 通过定时任务，实现延迟 delay 毫秒，再次执行获得令牌。
                commandExecutor.getConnectionManager().getGroup().schedule(() -> {
                    tryAcquireAsync(permits, promise, timeoutInMillis);
                }, delay, TimeUnit.MILLISECONDS);
                return;
            }

            // 计算剩余可获取令牌的时间
            long el = System.currentTimeMillis() - s;
            long remains = timeoutInMillis - el;
            // 如果无剩余时间，则 return 并通过 promise 返回获得锁失败。
            if (remains <= 0) {
                promise.trySuccess(false);
                return;
            }
            // 剩余时间小于锁刷新时间，则最后还是尝试一次延迟 remains 毫秒，再次执行获得锁
            if (remains < delay) {
                commandExecutor.getConnectionManager().getGroup().schedule(() -> {
                    promise.trySuccess(false);
                }, remains, TimeUnit.MILLISECONDS);
            } else {
                // 剩余时间大于锁刷新时间，则延迟 delay 毫秒，再次执行获得锁
                long start = System.currentTimeMillis();
                commandExecutor.getConnectionManager().getGroup().schedule(() -> {
                    // 因为定时器是延迟 delay 毫秒，实际可能超过 remains 毫秒，此处判断兜底，避免无效重试。
                    long elapsed = System.currentTimeMillis() - start;
                    if (remains <= elapsed) {
                        promise.trySuccess(false);
                        return;
                    }

                    tryAcquireAsync(permits, promise, remains - elapsed);
                }, delay, TimeUnit.MILLISECONDS);
            }
        });
    }

    private <T> RFuture<T> tryAcquireAsync(RedisCommand<T> command, Long value) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, command,
                "local rate = redis.call('hget', KEYS[1], 'rate');"
              + "local interval = redis.call('hget', KEYS[1], 'interval');"
              + "local type = redis.call('hget', KEYS[1], 'type');"
              + "assert(rate ~= false and interval ~= false and type ~= false, 'RateLimiter is not initialized')"

              + "local valueName = KEYS[2];"
              + "if type == '1' then "
                  + "valueName = KEYS[3];"
              + "end;"

              + "local currentValue = redis.call('get', valueName); "
              + "if currentValue ~= false then "
                     + "if tonumber(currentValue) < tonumber(ARGV[1]) then "
                         + "return redis.call('pttl', valueName); "
                     + "else "
                         + "redis.call('decrby', valueName, ARGV[1]); "
                         + "return nil; "
                     + "end; "
              + "else "
                     + "assert(tonumber(rate) >= tonumber(ARGV[1]), 'Requested permits amount could not exceed defined rate'); "
                     + "redis.call('set', valueName, rate, 'px', interval); "
                     + "redis.call('decrby', valueName, ARGV[1]); "
                     + "return nil; "
              + "end;",
                Arrays.<Object>asList(getName(), getValueName(), getClientValueName()), // keys [限流器的名字，限流器的值的名字，Client 编号的名字]
                value, commandExecutor.getConnectionManager().getId()); // values [需要获取令牌的数量，Client 编号]
    }

    @Override
    public boolean trySetRate(RateType type, long rate, long rateInterval, RateIntervalUnit unit) {
        return get(trySetRateAsync(type, rate, rateInterval, unit));
    }

    @Override
    public RFuture<Boolean> trySetRateAsync(RateType type, long rate, long rateInterval, RateIntervalUnit unit) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "redis.call('hsetnx', KEYS[1], 'rate', ARGV[1]);"
              + "redis.call('hsetnx', KEYS[1], 'interval', ARGV[2]);"
              + "return redis.call('hsetnx', KEYS[1], 'type', ARGV[3]);",
                Collections.<Object>singletonList(getName()), // keys [分布锁名]
                rate, unit.toMillis(rateInterval), type.ordinal()); // values [速度、速度单位、限流类型]
    }

    private static final RedisCommand HGETALL = new RedisCommand("HGETALL", new MultiDecoder<RateLimiterConfig>() {
        @Override
        public Decoder<Object> getDecoder(int paramNum, State state) {
            return null;
        }

        @Override
        public RateLimiterConfig decode(List<Object> parts, State state) {
            Map<String, String> map = new HashMap<>(parts.size()/2);
            for (int i = 0; i < parts.size(); i++) {
                if (i % 2 != 0) {
                    map.put(parts.get(i-1).toString(), parts.get(i).toString());
                }
            }

            // 创建 RateType 对象
            RateType type = RateType.values()[Integer.valueOf(map.get("type"))];
            Long rateInterval = Long.valueOf(map.get("interval"));
            Long rate = Long.valueOf(map.get("rate"));
            return new RateLimiterConfig(type, rateInterval, rate);
        }

    }, ValueType.MAP);

    @Override
    public RateLimiterConfig getConfig() {
        return get(getConfigAsync());
    }

    @Override
    public RFuture<RateLimiterConfig> getConfigAsync() {
        return commandExecutor.readAsync(getName(), StringCodec.INSTANCE, HGETALL, getName());
    }

}
