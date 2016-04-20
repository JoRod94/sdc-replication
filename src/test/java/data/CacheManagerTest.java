package data;

import junit.framework.Assert;

/**
 * Created by frm on 20/04/16.
 */
public class CacheManagerTest implements Cacheable {
    private int id;

    public CacheManagerTest(int i) {
        id = i;
    }

    @Override
    public String getId() {
        return Integer.toString(id);
    }

    public static void main(String[] args) {
        CacheManager<CacheManagerTest> cache = new CacheManager<>(20);
        for(int i = 1; i < 22; i++) {
            cache.add(new CacheManagerTest(i));
            /* Final result is clear enough,
             * but uncomment to see the cache development
            if( cache.get(Integer.toString(1)) != null)
                System.out.println(i + ": Has 1");
            else
                System.out.println(i + ": No 1");
             */
        }

        Assert.assertEquals(null, cache.get(Integer.toString(1)));
    }
}
