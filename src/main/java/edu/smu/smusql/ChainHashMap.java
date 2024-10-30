// package edu.smu.smusql;

// import java.util.*;
// import java.util.function.BiConsumer;
// import java.util.function.BiFunction;
// import java.util.function.Function;
// import java.util.function.Predicate;
// import java.util.Map.*;
// import edu.smu.smusql.UnsortedTableMap;


// // Efficient HashMap implementation using bucket chaining
// public class ChainHashMap<K, V> extends AbstractMap<K,V> {

//     // Stores table rows
//     // ID --> converted to hashcode
//     // Each pos in ArrayList is a bucket
//     // private ArrayList<HashMap<String, HashMap<String, String>>> table = new ArrayList<HashMap<String, HashMap<String, String>>>(); 
//     private UnsortedTableMap<K, V>[] table = new UnsortedTableMap[10];

//     public static void main(String[] args) {
        
//     }
//     // @Override
//     // public V get(Object key) {
//     //     // TODO Auto-generated method stub
//     //     return super.get(key);
//     // }

//     // @Override
//     // public int hashCode() {
//     //     // TODO Auto-generated method stub
//     //     return super.hashCode();
//     // }

//     // @Override
//     // public Set<K> keySet() {
//     //     // TODO Auto-generated method stub
//     //     return super.keySet();
//     // }

//     // @Override
//     // public V put(K key, V value) {
//     //     // TODO Auto-generated method stub
//     //     return super.put(key, value);
//     // }

//     // @Override
//     // public V remove(Object key) {
//     //     // TODO Auto-generated method stub
//     //     return super.remove(key);
//     // }

//     // @Override
//     // public int size() {
//     //     // TODO Auto-generated method stub
//     //     return super.size();
//     // }

//     // @Override
//     // public Collection<V> values() {
//     //     // TODO Auto-generated method stub
//     //     return super.values();
//     // }

// }