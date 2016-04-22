package data;

import java.util.HashMap;
import java.util.PriorityQueue;

/**
 * Created by frm on 18/04/16.
 */
public class CacheManager<T extends Cacheable>{
    private final int maxSize;
    private HashMap<Integer, CacheObject> cache;
    private PriorityQueue<CacheObject> queue;

    /**
     * Creates a CacheManager with the given maximum size.
     * Any entry added after reaching the maximum size will cause
     * for the oldest entry to be removed.
     * @param size
     */
    public CacheManager(int size) {
        maxSize = size;
        cache = new HashMap<>();
        queue = new PriorityQueue<>();
    }

    /**
     * Returns a Cacheable object with the given id.
     * If the object is not found in cache, null is returned
     * @param id - id of the object to search for
     * @return - queried object or null
     */
    public T get(int id) {
        CacheObject o = cache.get(id);
        return o == null ? null : (T) o.getContent();
    }

    /**
     * Adds a given object to the cache
     * @param o - object to be cached
     */
    public void add(T o) {
        if(maxSize == cache.size()) {
            int targetId = ((T)queue.poll().getContent()).getId();
            cache.remove(targetId);
        }

        CacheObject co = new CacheObject(o);
        cache.put(o.getId(), co);
        queue.add(co);
    }

    /**
     * Class that wraps the its content allowing it to be compared using a timestamp
     * Timestamps allow for the object to be organized using a queue,
     * so that the oldest object in cache is removed
     */
    class CacheObject implements Comparable<CacheObject> {
        private Object object;
        private Long timestamp;

        /**
         * Creates a CacheObject that wraps the given object
         * @param o - object to be wrapped
         */
        public CacheObject(Object o) {
            this.object = o;
            this.timestamp = System.currentTimeMillis() % 1000;
        }

        /**
         * Returns the wrapped object
         * @return - wrapped object
         */
        public Object getContent() {
            return object;
        }

        @Override
        public int compareTo(CacheObject co) {
            return Long.compare(timestamp, co.timestamp);
        }
    }
}
