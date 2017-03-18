package assign2;

import java.util.Map;

/**
 * Created by cianduffy on 18/03/2017.
 */
public interface ReduceCallback<E, K, V> {
    void reduceDone(E e, Map<K, V> results);
}
