package org.jusnga.pageviews.cache;

import java.util.Optional;

public interface ResultCache<K, V> extends AutoCloseable {
    void cacheResult(K key, V result);

    Optional<V> getCachedResult(K key);
}
