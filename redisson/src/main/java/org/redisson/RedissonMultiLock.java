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

import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.RedisResponseTimeoutException;
import org.redisson.misc.RPromise;
import org.redisson.misc.RedissonPromise;
import org.redisson.misc.TransferListener;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;

/**
 * Groups multiple independent locks and manages them as one lock.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonMultiLock implements RLock {

    class LockState {

        private final long newLeaseTime;
        private final long lockWaitTime;
        private final List<RLock> acquiredLocks;
        private final long waitTime;
        private final long threadId;
        private final long leaseTime;
        private final TimeUnit unit;

        private long remainTime;
        private long time = System.currentTimeMillis();
        private int failedLocksLimit;

        LockState(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
            this.waitTime = waitTime;
            this.leaseTime = leaseTime;
            this.unit = unit;
            this.threadId = threadId;

            // 计算新的锁的时长 newLeaseTime
            if (leaseTime != -1) {
                if (waitTime == -1) {
                    newLeaseTime = unit.toMillis(leaseTime);
                } else {
                    newLeaseTime = unit.toMillis(waitTime) * 2;
                }
            } else {
                newLeaseTime = -1;
            }

            // 计算剩余等待锁的时间 remainTime
            remainTime = -1;
            if (waitTime != -1) {
                remainTime = unit.toMillis(waitTime);
            }
            // 计算每个锁的等待时间
            lockWaitTime = calcLockWaitTime(remainTime);

            // 允许获得锁失败的次数
            failedLocksLimit = failedLocksLimit();
            // 已经获得到锁的数组
            acquiredLocks = new ArrayList<>(locks.size());
        }

        void tryAcquireLockAsync(ListIterator<RLock> iterator, RPromise<Boolean> result) {
            // 如果迭代 iterator 的尾部，则重新设置每个锁的过期时间
            if (!iterator.hasNext()) {
                checkLeaseTimeAsync(result);
                return;
            }

            // 获得下一个 RLock 对象
            RLock lock = iterator.next();
            // 创建 RPromise 对象
            RPromise<Boolean> lockAcquiredFuture = new RedissonPromise<Boolean>();
            // 如果等待时间 waitTime 为 -1（不限制），并且锁时长为 -1（不限制），则使用 #tryLock() 方法。
            if (waitTime == -1 && leaseTime == -1) {
                lock.tryLockAsync(threadId)
                    .onComplete(new TransferListener<Boolean>(lockAcquiredFuture));
            // 如果任一不为 -1 时，则计算新的等待时间 awaitTime ，然后调用 #tryLock(long waitTime, long leaseTime, TimeUnit unit) 方法。
            } else {
                long awaitTime = Math.min(lockWaitTime, remainTime);
                lock.tryLockAsync(awaitTime, newLeaseTime, TimeUnit.MILLISECONDS, threadId)
                    .onComplete(new TransferListener<Boolean>(lockAcquiredFuture));
            }

            lockAcquiredFuture.onComplete((res, e) -> {
                // 如果 res 非空，设置 lockAcquired 是否获得到锁
                boolean lockAcquired = false;
                if (res != null) {
                    lockAcquired = res;
                }

                // 发生响应超时。因为无法确定实际是否获得到锁，所以直接释放当前 RLock
                if (e instanceof RedisResponseTimeoutException) {
                    unlockInnerAsync(Collections.singletonList(lock), threadId);
                }

                // 如果获得成功，则添加到 acquiredLocks 数组中
                if (lockAcquired) {
                    acquiredLocks.add(lock);
                } else {
                    // 如果已经到达最少需要获得锁的数量，则直接 break 。例如说，RedLock 只需要获得 N / 2 + 1 把。
                    if (locks.size() - acquiredLocks.size() == failedLocksLimit()) {
                        checkLeaseTimeAsync(result);
                        return;
                    }

                    // 当已经没有允许失败的数量，则进行相应的处理
                    if (failedLocksLimit == 0) {
                        // 创建释放所有的锁的 Future
                        unlockInnerAsync(acquiredLocks, threadId).onComplete((r, ex) -> {
                            // 如果发生异常，则通过 result 回调异常
                            if (ex != null) {
                                result.tryFailure(ex);
                                return;
                            }

                            // 如果未设置阻塞时间，直接通知 result 失败。因为是 tryLock ，只是尝试加锁一次，不会无限重试。
                            if (waitTime == -1) {
                                result.trySuccess(false);
                                return;
                            }

                            // 重置整个获得锁的过程，在剩余的时间里，重新来一遍
                            // 重置 failedLocksLimit 变量
                            failedLocksLimit = failedLocksLimit();
                            // 重置 acquiredLocks 为空
                            acquiredLocks.clear();
                            // reset iterator
                            // 重置 iterator 设置回迭代器的头
                            while (iterator.hasPrevious()) {
                                iterator.previous();
                            }

                            // 校验剩余时间是否足够
                            checkRemainTimeAsync(iterator, result);
                        });
                        return;
                    } else {
                        failedLocksLimit--;
                    }
                }

                // 校验剩余时间是否足够
                checkRemainTimeAsync(iterator, result);
            });
        }

        private void checkLeaseTimeAsync(RPromise<Boolean> result) {
            // 如果设置了锁的过期时间 leaseTime ，则重新设置每个锁的过期时间
            if (leaseTime != -1) {
                // 创建 AtomicInteger 计数器，用于回调逻辑的计数，从而判断是不是所有回调都执行完了
                AtomicInteger counter = new AtomicInteger(acquiredLocks.size());
                // 遍历 acquiredLocks 数组，逐个设置过期时间
                for (RLock rLock : acquiredLocks) {
                    // 创建异步设置过期时间的 RFuture
                    RFuture<Boolean> future = ((RedissonLock) rLock).expireAsync(unit.toMillis(leaseTime), TimeUnit.MILLISECONDS);
                    future.onComplete((res, e) -> {
                        // 如果发生异常，则通过 result 回调异常
                        if (e != null) {
                            result.tryFailure(e);
                            return;
                        }

                        // 如果全部成功，则通过 result 回调加锁成功
                        if (counter.decrementAndGet() == 0) {
                            result.trySuccess(true);
                        }
                    });
                }
                return;
            }

            // 如果未设置了锁的过期时间 leaseTime ，则通过 result 回调加锁成功
            result.trySuccess(true);
        }

        private void checkRemainTimeAsync(ListIterator<RLock> iterator, RPromise<Boolean> result) {
            // 如果设置了等待超时时间，计算剩余时间 remainTime
            if (remainTime != -1) {
                remainTime += -(System.currentTimeMillis() - time);
                // 记录新的当前时间
                time = System.currentTimeMillis();
                // 如果没有剩余时间，意味着已经超时，释放所有加载成功的锁
                if (remainTime <= 0) {
                    // 创建释放所有已获得到锁们的 Future
                    unlockInnerAsync(acquiredLocks, threadId).onComplete((res, e) -> {
                        // 如果发生异常，则通过 result 回调异常
                        if (e != null) {
                            result.tryFailure(e);
                            return;
                        }

                        // 如果全部成功，则通过 result 回调加锁成功
                        result.trySuccess(false);
                    });
                    // return 返回结束
                    return;
                }
            }

            // 如果未设置等待超时时间，则继续加锁下一个 RLock
            tryAcquireLockAsync(iterator, result);
        }

    }

    /**
     * RLock 数组
     */
    final List<RLock> locks = new ArrayList<>();

    /**
     * Creates instance with multiple {@link RLock} objects.
     * Each RLock object could be created by own Redisson instance.
     *
     * @param locks - array of locks
     */
    public RedissonMultiLock(RLock... locks) {
        if (locks.length == 0) {
            throw new IllegalArgumentException("Lock objects are not defined");
        }
        this.locks.addAll(Arrays.asList(locks));
    }

    @Override
    public void lock() {
        try {
            lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void lock(long leaseTime, TimeUnit unit) {
        try {
            lockInterruptibly(leaseTime, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit) {
        return lockAsync(leaseTime, unit, Thread.currentThread().getId());
    }

    @Override
    public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit, long threadId) {
        // 计算 waitTime 时间
        long baseWaitTime = locks.size() * 1500;
        long waitTime;
        if (leaseTime == -1) { // 如果未设置超时时间，则直接使用 baseWaitTime
            waitTime = baseWaitTime;
        } else {
            leaseTime = unit.toMillis(leaseTime);
            waitTime = leaseTime;
            if (waitTime <= 2000) { // 保证最小 waitTime 时间是 2000
                waitTime = 2000;
            } else if (waitTime <= baseWaitTime) { // 在 [waitTime / 2, waitTime) 之间随机
                waitTime = ThreadLocalRandom.current().nextLong(waitTime / 2, waitTime);
            } else { // 在 [baseWaitTime, waitTime) 之间随机
                waitTime = ThreadLocalRandom.current().nextLong(baseWaitTime, waitTime);
            }
        }

        // 创建 RPromise 对象
        RPromise<Void> result = new RedissonPromise<Void>();
        // 执行异步加锁
        tryLockAsync(threadId, leaseTime, TimeUnit.MILLISECONDS, waitTime, result);
        return result;
    }

    protected void tryLockAsync(long threadId, long leaseTime, TimeUnit unit, long waitTime, RPromise<Void> result) {
        // 执行异步加锁锁
        tryLockAsync(waitTime, leaseTime, unit, threadId).onComplete((res, e) -> {
            // 如果发生异常，则通过 result 回调异常
            if (e != null) {
                result.tryFailure(e);
                return;
            }

            // 如果加锁成功，则通知 result 成功
            if (res) {
                result.trySuccess(null);
            // 如果加锁失败，则递归调用 tryLockAsync 方法
            } else {
                tryLockAsync(threadId, leaseTime, unit, waitTime, result);
            }
        });
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        lockInterruptibly(-1, null);
    }

    @Override
    public void lockInterruptibly(long leaseTime, TimeUnit unit) throws InterruptedException {
        // 计算 waitTime 时间
        long baseWaitTime = locks.size() * 1500;
        long waitTime = -1;
        if (leaseTime == -1) { // 如果未设置超时时间，则直接使用 baseWaitTime
            waitTime = baseWaitTime;
        } else {
            leaseTime = unit.toMillis(leaseTime);
            waitTime = leaseTime;
            if (waitTime <= 2000) { // 保证最小 waitTime 时间是 2000
                waitTime = 2000;
            } else if (waitTime <= baseWaitTime) { // 在 [waitTime / 2, waitTime) 之间随机
                waitTime = ThreadLocalRandom.current().nextLong(waitTime / 2, waitTime);
            } else { // 在 [baseWaitTime, waitTime) 之间随机
                waitTime = ThreadLocalRandom.current().nextLong(baseWaitTime, waitTime);
            }
        }

        // 死循环，直到加锁成功
        while (true) {
            if (tryLock(waitTime, leaseTime, TimeUnit.MILLISECONDS)) {
                return;
            }
        }
    }

    @Override
    public boolean tryLock() {
        try {
            // 同步获得锁
            return tryLock(-1, -1, null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    protected void unlockInner(Collection<RLock> locks) {
        // 创建 RFuture 数组
        List<RFuture<Void>> futures = new ArrayList<>(locks.size());

        // 逐个创建异步解锁 Future，并添加到 futures 数组中
        for (RLock lock : locks) {
            futures.add(lock.unlockAsync());
        }

        // 同步阻塞 futures ，全部释放完成
        for (RFuture<Void> unlockFuture : futures) {
            unlockFuture.awaitUninterruptibly();
        }
    }

    protected RFuture<Void> unlockInnerAsync(Collection<RLock> locks, long threadId) {
        // 如果 locks 为空，直接返回成功的 RFuture
        if (locks.isEmpty()) {
            return RedissonPromise.newSucceededFuture(null);
        }

        // 创建 RPromise 对象
        RPromise<Void> result = new RedissonPromise<Void>();
        // 创建 AtomicInteger 计数器，用于回调逻辑的计数，从而判断是不是所有回调都执行完了
        AtomicInteger counter = new AtomicInteger(locks.size());
        // 遍历 acquiredLocks 数组，逐个解锁
        for (RLock lock : locks) {
            lock.unlockAsync(threadId).onComplete((res, e) -> {
                // 如果发生异常，则通过 result 回调异常
                if (e != null) {
                    result.tryFailure(e);
                    return;
                }

                // 如果全部成功，则通过 result 回调解锁成功
                if (counter.decrementAndGet() == 0) {
                    result.trySuccess(null);
                }
            });
        }
        return result;
    }

    @Override
    public boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException {
        return tryLock(waitTime, -1, unit);
    }

    protected int failedLocksLimit() {
        return 0;
    }

    @Override
    public boolean tryLock(long waitTime, long leaseTime, TimeUnit unit) throws InterruptedException {
//        try {
//            return tryLockAsync(waitTime, leaseTime, unit).get();
//        } catch (ExecutionException e) {
//            throw new IllegalStateException(e);
//        }
        // 计算新的锁的时长 newLeaseTime
        long newLeaseTime = -1;
        if (leaseTime != -1) {
            if (waitTime == -1) { // 如果无限等待，则直接使用 leaseTime 即可。
                newLeaseTime = unit.toMillis(leaseTime);
            } else { // 如果设置了等待时长，则为等待时间 waitTime * 2 。不知道为什么要 * 2 ？例如说，先获得到了第一个锁，然后在获得第二个锁的时候，阻塞等待了 waitTime ，那么可能第一个锁就已经自动过期，所以 * 2 避免这个情况。
                newLeaseTime = unit.toMillis(waitTime) * 2;
            }
        }

        long time = System.currentTimeMillis();
        // 计算剩余等待锁的时间 remainTime
        long remainTime = -1;
        if (waitTime != -1) {
            remainTime = unit.toMillis(waitTime);
        }
        // 计算每个锁的等待时间
        long lockWaitTime = calcLockWaitTime(remainTime);

        // 允许获得锁失败的次数
        int failedLocksLimit = failedLocksLimit();
        // 已经获得到锁的数组
        List<RLock> acquiredLocks = new ArrayList<>(locks.size());
        // 遍历 RLock 数组，逐个获得锁
        for (ListIterator<RLock> iterator = locks.listIterator(); iterator.hasNext();) {
            // 当前 RLock
            RLock lock = iterator.next();
            boolean lockAcquired; // 标记是否获得到锁
            try {
                // 如果等待时间 waitTime 为 -1（不限制），并且锁时长为 -1（不限制），则使用 #tryLock() 方法。
                if (waitTime == -1 && leaseTime == -1) {
                    lockAcquired = lock.tryLock();
                // 如果任一不为 -1 时，则计算新的等待时间 awaitTime ，然后调用 #tryLock(long waitTime, long leaseTime, TimeUnit unit) 方法。
                } else {
                    long awaitTime = Math.min(lockWaitTime, remainTime);
                    lockAcquired = lock.tryLock(awaitTime, newLeaseTime, TimeUnit.MILLISECONDS);
                }
            } catch (RedisResponseTimeoutException e) {
                // 发生响应超时。因为无法确定实际是否获得到锁，所以直接释放当前 RLock
                unlockInner(Collections.singletonList(lock));
                // 标记未获得锁
                lockAcquired = false;
            } catch (Exception e) {
                // 标记未获得锁
                lockAcquired = false;
            }

            // 如果获得成功，则添加到 acquiredLocks 数组中
            if (lockAcquired) {
                acquiredLocks.add(lock);
            } else {
                // 如果已经到达最少需要获得锁的数量，则直接 break 。例如说，RedLock 只需要获得 N / 2 + 1 把。
                if (locks.size() - acquiredLocks.size() == failedLocksLimit()) {
                    break;
                }

                // 当已经没有允许失败的数量，则进行相应的处理
                if (failedLocksLimit == 0) {
                    // 释放所有的锁
                    unlockInner(acquiredLocks);
                    // 如果未设置阻塞时间，直接返回 false ，表示失败。因为是 tryLock ，只是尝试加锁一次，不会无限重试。
                    if (waitTime == -1) {
                        return false;
                    }
                    // 重置整个获得锁的过程，在剩余的时间里，重新来一遍
                    // 重置 failedLocksLimit 变量
                    failedLocksLimit = failedLocksLimit();
                    // 重置 acquiredLocks 为空
                    acquiredLocks.clear();
                    // reset iterator
                    // 重置 iterator 设置回迭代器的头
                    while (iterator.hasPrevious()) {
                        iterator.previous();
                    }
                // failedLocksLimit 减一
                } else {
                    failedLocksLimit--;
                }
            }

            // 计算剩余时间 remainTime
            if (remainTime != -1) {
                remainTime -= System.currentTimeMillis() - time;
                // 记录新的当前时间
                time = System.currentTimeMillis();
                // 如果没有剩余时间，意味着已经超时，释放所有加载成功的锁，并返回 false
                if (remainTime <= 0) {
                    unlockInner(acquiredLocks);
                    return false;
                }
            }
        }

        // 如果设置了锁的过期时间 leaseTime ，则重新设置每个锁的过期时间
        if (leaseTime != -1) {
            // 遍历 acquiredLocks 数组，创建异步设置过期时间的 Future
            List<RFuture<Boolean>> futures = new ArrayList<>(acquiredLocks.size());
            for (RLock rLock : acquiredLocks) {
                RFuture<Boolean> future = ((RedissonLock) rLock).expireAsync(unit.toMillis(leaseTime), TimeUnit.MILLISECONDS);
                futures.add(future);
            }

            // 阻塞等待所有 futures 完成
            for (RFuture<Boolean> rFuture : futures) {
                rFuture.syncUninterruptibly();
            }
        }

        // 返回 true ，表示加锁成功
        return true;
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
        // 创建 RPromise 对象，用于通知结果
        RPromise<Boolean> result = new RedissonPromise<Boolean>();
        // 创建 LockState 对象
        LockState state = new LockState(waitTime, leaseTime, unit, threadId);
        // 发起异步加锁
        state.tryAcquireLockAsync(locks.listIterator(), result);
        // 返回结果
        return result;
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit) {
        return tryLockAsync(waitTime, leaseTime, unit, Thread.currentThread().getId());
    }

    protected long calcLockWaitTime(long remainTime) {
        return remainTime;
    }

    @Override
    public RFuture<Void> unlockAsync(long threadId) {
        return unlockInnerAsync(locks, threadId);
    }

    @Override
    public void unlock() {
        // 创建 RFuture 数组
        List<RFuture<Void>> futures = new ArrayList<>(locks.size());

        // 逐个创建异步解锁 Future，并添加到 futures 数组中
        for (RLock lock : locks) {
            futures.add(lock.unlockAsync());
        }

        // 同步阻塞 futures ，全部释放完成
        for (RFuture<Void> future : futures) {
            future.syncUninterruptibly();
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Void> unlockAsync() {
        return unlockAsync(Thread.currentThread().getId());
    }

    @Override
    public RFuture<Boolean> tryLockAsync() {
        return tryLockAsync(Thread.currentThread().getId());
    }

    @Override
    public RFuture<Void> lockAsync() {
        return lockAsync(Thread.currentThread().getId());
    }

    @Override
    public RFuture<Void> lockAsync(long threadId) {
        return lockAsync(-1, null, threadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long threadId) {
        return tryLockAsync(-1, -1, null, threadId);
    }

    @Override
    public RFuture<Boolean> tryLockAsync(long waitTime, TimeUnit unit) {
        return tryLockAsync(waitTime, -1, unit);
    }

    @Override
    public RFuture<Integer> getHoldCountAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean forceUnlock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLocked() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> isLockedAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isHeldByThread(long threadId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isHeldByCurrentThread() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getHoldCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Long> remainTimeToLiveAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long remainTimeToLive() {
        throw new UnsupportedOperationException();
    }

}
