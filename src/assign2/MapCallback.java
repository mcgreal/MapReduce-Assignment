package assign2;

import java.util.List;

/**
 * Created by cianduffy on 18/03/2017.
 */
public interface MapCallback<E, V> {
    void mapDone(E key, List<V> values);
}
