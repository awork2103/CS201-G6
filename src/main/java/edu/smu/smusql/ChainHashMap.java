import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

// Efficient HashMap implementation using bucket chaining
public class ChainHashMap<K, V> extends AbstractMap<K,V> {

    private ArrayList<MapEntry<K,V>> table = new ArrayList<>();


    @Override
    public V get(Object key) {
        // TODO Auto-generated method stub
        return super.get(key);
    }

    @Override
    public int hashCode() {
        // TODO Auto-generated method stub
        return super.hashCode();
    }

    @Override
    public Set<K> keySet() {
        // TODO Auto-generated method stub
        return super.keySet();
    }

    @Override
    public V put(K key, V value) {
        // TODO Auto-generated method stub
        return super.put(key, value);
    }

    @Override
    public V remove(Object key) {
        // TODO Auto-generated method stub
        return super.remove(key);
    }

    @Override
    public int size() {
        // TODO Auto-generated method stub
        return super.size();
    }

    @Override
    public Collection<V> values() {
        // TODO Auto-generated method stub
        return super.values();
    }

}