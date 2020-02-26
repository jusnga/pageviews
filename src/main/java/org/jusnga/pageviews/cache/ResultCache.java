package org.jusnga.pageviews.cache;

import java.util.Optional;

public interface ResultCache<K, V> {
    void cacheResult(K key, V result);

    Optional<V> getCachedResult(K key);
}
