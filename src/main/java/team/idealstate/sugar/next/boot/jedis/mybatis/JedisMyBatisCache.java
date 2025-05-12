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

import lombok.Data;
import lombok.NonNull;
import org.apache.ibatis.cache.Cache;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import team.idealstate.sugar.internal.com.fasterxml.jackson.core.JsonFactory;
import team.idealstate.sugar.internal.com.fasterxml.jackson.databind.ObjectMapper;
import team.idealstate.sugar.next.boot.jedis.JedisProvider;
import team.idealstate.sugar.next.function.Lazy;
import team.idealstate.sugar.next.function.closure.Function;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static team.idealstate.sugar.next.function.Functional.lazy;

@Data
final class JedisMyBatisCache implements Cache {

    @NonNull
    private final String id;
    @NonNull
    private final JedisProvider jedisProvider;
    private final Integer expired;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private Object execute(Function<Jedis, Object> callback) {
        try (Jedis jedis = jedisProvider.getJedisPool().getResource()) {
            return callback.call(jedis);
        } catch (Throwable e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new JedisException(e);
        }
    }

    @Override
    public int getSize() {
        return (Integer) execute(jedis -> {
            Map<byte[], byte[]> result = jedis.hgetAll(getId().getBytes());
            return result.size();
        });
    }

    @Override
    public void putObject(final Object key, final Object value) {
        execute(jedis -> {
            final byte[] idBytes = getId().getBytes();
            jedis.hset(idBytes, key.toString().getBytes(), serialize(value));
            if (expired != null && jedis.ttl(idBytes) == -1) {
                jedis.expire(idBytes, expired);
            }
            return null;
        });
    }

    @Override
    public Object getObject(final Object key) {
        return execute(jedis ->
                deserialize(jedis.hget(getId().getBytes(), key.toString().getBytes())));
    }

    @Override
    public Object removeObject(final Object key) {
        return execute(jedis -> jedis.hdel(getId(), key.toString()));
    }

    @Override
    public void clear() {
        execute(jedis -> {
            jedis.del(getId());
            return null;
        });
    }

    private final Lazy<ObjectMapper> json = lazy(() -> new ObjectMapper(new JsonFactory()).findAndRegisterModules());

    private byte[] serialize(Object value) throws IOException {
        return json.get().writeValueAsBytes(value);
    }

    private Object deserialize(byte[] value) throws IOException {
        return json.get().readValue(value, Object.class);
    }
}
