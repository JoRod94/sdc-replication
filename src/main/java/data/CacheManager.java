package data;

import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by frm on 18/04/16.
 */
public class CacheManager<T extends Cacheable>{
    private final int maxSize;
    private HashMap<Integer, CacheObject> cache;
    private PriorityQueue<CacheObject> queue;
    private ReentrantLock lock;

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
        lock = new ReentrantLock();
    }

    /**
     * Returns a Cacheable object with the given id.
     * If the object is not found in cache, null is returned
     * @param id - id of the object to search for
     * @return - queried object or null
     */
    public T get(int id) {
        lock.lock();
        CacheObject o = cache.get(id);
        lock.unlock();
        T t = o == null ? null : (T) o.touch().getContent();

        if(o != null) {
            // Reordering the priority queue.
            // It's ok to remove after updating the object,
            // as it won't affect the search for the object.
            // The removal criteria will be the result of equals.
            // Since there is no override, it will use the object id.
            lock.lock();
            queue.remove(o);
            queue.add(o);
            lock.unlock();
        }

        return t;
    }

    /**
     * Adds a given object to the cache
     * @param o - object to be cached
     */
    public void add(T o) {
        lock.lock();
        if(maxSize == cache.size()) {
            int targetId = ((T)queue.poll().getContent()).getId();
            cache.remove(targetId);
        }

        CacheObject co = new CacheObject(o);
        cache.put(o.getId(), co);
        queue.add(co);
        lock.unlock();
    }

    /**
     * Class that wraps the its content allowing it to be compared using a timestamp
     * Timestamps allow for the object to be organized using a queue,
     * so that the oldest object in cache is removed
     */
    static class CacheObject implements Comparable<CacheObject> {
        private Object object;
        private Long timestamp;
        private Long lastAccessed;

        private static double TIME_WEIGHT = 0.7;
        private static double ACCESS_WEIGHT = 0.3;

        /**
         * Creates a CacheObject that wraps the given object
         * @param o - object to be wrapped
         */
        public CacheObject(Object o) {
            this.object = o;
            this.timestamp = System.currentTimeMillis();
            this.lastAccessed = timestamp;
        }

        /**
         * Updates the object access timestamp
         * Returns itself to allow for method chaining
         * @return own object
         */
        public CacheObject touch() {
            this.lastAccessed = System.currentTimeMillis();
            return this;
        }

        /**
         * Returns the wrapped object
         * @return - wrapped object
         */
        public Object getContent() {
            return object;
        }

        /**
         * Calculates the overall weight.
         * Amounts for weights on last access and creation timestamps
         * @return - weight factor
         */
        private Double weightFactor() {
            return ACCESS_WEIGHT * lastAccessed + TIME_WEIGHT * timestamp;
        }

        @Override
        public int compareTo(CacheObject co) {
            // This comparison version accounts for temporal locality.
            // It's just to allow to take advantage of this property
            // in a very, very simple way, as this wasn't really the point of the project.
            return Double.compare(weightFactor(), co.weightFactor());
        }
    }
}
