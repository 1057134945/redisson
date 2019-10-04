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

import org.redisson.misc.RPromise;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

public class RedissonLockEntry implements PubSubEntry<RedissonLockEntry> {

    /**
     * 计数器
     *
     * 每次发起订阅，则计数器 + 1
     * 每次取消订阅，则计数器 - 1 。当减少到 0 时，才正常取消订阅。
     */
    private int counter;

    /**
     * 信号量，用于实现 RedissonLock 阻塞等待的通知
     */
    private final Semaphore latch;
    private final RPromise<RedissonLockEntry> promise;
    /**
     * 监听器们
     */
    private final ConcurrentLinkedQueue<Runnable> listeners = new ConcurrentLinkedQueue<Runnable>();

    public RedissonLockEntry(RPromise<RedissonLockEntry> promise) {
        super();
        this.latch = new Semaphore(0);
        this.promise = promise;
    }

    @Override
    public void aquire() {
        counter++;
    }

    @Override
    public int release() {
        return --counter;
    }

    @Override
    public RPromise<RedissonLockEntry> getPromise() {
        return promise;
    }

    public void addListener(Runnable listener) {
        listeners.add(listener);
    }

    public boolean removeListener(Runnable listener) {
        return listeners.remove(listener);
    }

    public ConcurrentLinkedQueue<Runnable> getListeners() {
        return listeners;
    }

    public Semaphore getLatch() {
        return latch;
    }

}
