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
package org.redisson.api;

/**
 * 限流类型
 *
 * @author Nikita Koksharov
 */
public enum RateType {

    /**
     * Total rate for all RateLimiter instances
     *
     * 相同名字的所有 RateLimiter 实例
     */
    OVERALL,

    /**
     * Total rate for all RateLimiter instances working with the same Redisson instance
     *
     * 相同 JVM 进程的所有相同名字的所有 RateLimiter 实例
     */
    PER_CLIENT

}
