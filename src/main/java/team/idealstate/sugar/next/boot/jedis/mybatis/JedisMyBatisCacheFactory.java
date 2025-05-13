/*
 *    Copyright 2025 ideal-state
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package team.idealstate.sugar.next.boot.jedis.mybatis;

import org.apache.ibatis.cache.Cache;
import team.idealstate.sugar.next.boot.jedis.JedisProvider;
import team.idealstate.sugar.next.boot.mybatis.spi.CacheFactory;
import team.idealstate.sugar.next.context.annotation.component.Component;
import team.idealstate.sugar.next.context.annotation.feature.Autowired;
import team.idealstate.sugar.next.context.annotation.feature.DependsOn;
import team.idealstate.sugar.validate.Validation;
import team.idealstate.sugar.validate.annotation.NotNull;

@Component
@DependsOn(
        properties = {
            @DependsOn.Property(key = "team.idealstate.sugar.next.boot.jedis.annotation.EnableJedis", strict = false),
            @DependsOn.Property(
                    key = "team.idealstate.sugar.next.boot.mybatis.annotation.EnableMyBatis",
                    strict = false)
        })
public class JedisMyBatisCacheFactory implements CacheFactory {

    @Override
    public Cache createCache(@NotNull String id, Integer expired) {
        Validation.notNull(id, "Id must not be null.");
        return new JedisMyBatisCache(id, getJedisProvider(), expired);
    }

    private volatile JedisProvider jedisProvider;

    @NotNull
    private JedisProvider getJedisProvider() {
        return Validation.requireNotNull(jedisProvider, "Jedis provider must not be null.");
    }

    @Autowired
    public void setJedisProvider(@NotNull JedisProvider jedisProvider) {
        this.jedisProvider = jedisProvider;
    }
}
