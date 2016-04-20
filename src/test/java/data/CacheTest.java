package data;

/**
 * Created by frm on 20/04/16.
 */
public class CacheTest implements Cacheable {
    private int id;

    public CacheTest(int i) {
        id = i;
    }

    @Override
    public String getId() {
        return Integer.toString(id);
    }

    public static void main(String[] args) {
        CacheManager<CacheTest> cache = new CacheManager<>(20);
        for(int i = 1; i < 22; i++) {
            cache.add(new CacheTest(i));
            if( cache.get(Integer.toString(1)) != null)
                System.out.println(i + ": Has 1");
            else
                System.out.println(i + ": No 1");
        }

        System.out.println("Should not have 1. Has 1? - " +
                Boolean.toString(cache.get(Integer.toString(1)) != null));
    }
}
