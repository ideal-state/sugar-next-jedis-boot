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
                @DependsOn.Property(key = "team.idealstate.sugar.next.boot.mybatis.annotation.EnableMyBatis", strict = false)
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
