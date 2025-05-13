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

import static team.idealstate.sugar.next.function.Functional.lazy;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Data;
import lombok.NonNull;
import org.apache.ibatis.cache.Cache;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import team.idealstate.sugar.internal.com.fasterxml.jackson.core.JsonFactory;
import team.idealstate.sugar.internal.com.fasterxml.jackson.databind.ObjectMapper;
import team.idealstate.sugar.logging.Log;
import team.idealstate.sugar.next.boot.jedis.JedisProvider;
import team.idealstate.sugar.next.function.Lazy;
import team.idealstate.sugar.next.function.closure.Function;
import team.idealstate.sugar.validate.annotation.NotNull;

@Data
public class JedisMyBatisCache implements Cache {

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
        Log.debug(() -> String.format("Getting cache size of [%s]...", getId()));
        return (Integer) execute(jedis -> {
            Map<byte[], byte[]> result = jedis.hgetAll(getId().getBytes());
            return result.size();
        });
    }

    @Override
    public void putObject(final Object key, final Object value) {
        Log.debug(() -> String.format("Putting cache [%s] with key [%s]...", getId(), key));
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
        Log.debug(() -> String.format("Getting cache [%s] with key [%s]...", getId(), key));
        return execute(jedis -> {
            byte[] value = jedis.hget(getId().getBytes(), key.toString().getBytes());
            if (value == null) {
                return null;
            }
            return deserialize(value);
        });
    }

    @Override
    public Object removeObject(final Object key) {
        Log.debug(() -> String.format("Removing cache [%s] with key [%s]...", getId(), key));
        return execute(jedis -> jedis.hdel(getId(), key.toString()));
    }

    @Override
    public void clear() {
        Log.debug(() -> String.format("Clearing cache [%s]...", getId()));
        execute(jedis -> {
            jedis.del(getId());
            return null;
        });
    }

    private final Lazy<ObjectMapper> json = lazy(() -> new ObjectMapper(new JsonFactory()).findAndRegisterModules());

    protected byte[] serialize(Object value) throws IOException {
        Log.debug(() -> String.format("Serializing cache [%s] with [%s]...", getId(), value));
        return json.get().writeValueAsBytes(value);
    }

    protected Object deserialize(@NotNull byte[] value) throws IOException {
        Log.debug(() -> String.format("Deserializing cache [%s] with [%s]...", getId(), value));
        return json.get().readValue(value, Object.class);
    }
}
