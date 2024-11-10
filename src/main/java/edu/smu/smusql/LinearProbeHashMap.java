package edu.smu.smusql;

import java.util.*;

public class LinearProbeHashMap<K, V> {

    private double loadFactor;
    private int hashMultiplier;
    private String hashingStrategy;
    private Entry<K, V>[] table;
    private int size;
    private static final int DEFAULT_CAPACITY = 11;

    // @SuppressWarnings("unchecked")
    public LinearProbeHashMap(int capacity, double loadFactor, int hashMultiplier, String hashingStrategy) {
        this.loadFactor = loadFactor;
        this.hashMultiplier = hashMultiplier;
        this.hashingStrategy = hashingStrategy;
        this.table = new Entry[capacity];
        this.size = 0;
    }

    public LinearProbeHashMap() {
        this(DEFAULT_CAPACITY, 0.75, 31, "POLYNOMIAL");
    }

    // Entry to store key-value pairs
    private static class Entry<K, V> {
        K key;
        V value;

        Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    public Set<K> keySet(){
        return null;
    }

    // Bitwise Hashing
    private int customBitwiseHashCode(K key) {
        // Typecast Key into a String (since ID is a string)
        String str = key.toString();
        int hash = 0;

        // Custom Hash Functiom with set multiplier
        for (int i = 0; i < str.length(); i++) {
            hash ^= ((int) str.charAt(i));
        }

        return hash;
    }

    // Polynomial Hashing
    private int customPolynomialHashCode(K key) {
        // Typecast Key into a String (since ID is a string)
        String str = key.toString();
        int hash = 0;

        // Custom Hash Functiom with set multiplier
        for (int i = 0; i < str.length(); i++) {
            hash *= hashMultiplier;
            hash += (int) str.charAt(i);
        }

        return hash;
    }

    // Cyclic Hashing
    private int customCyclicHashCode(K key) {
        // Typecast Key into a String (since ID is a string)
        String str = key.toString();
        int hash = 0;

        // Custom Hash Functiom with set multiplier
        for (int i = 0; i < str.length(); i++) {
            hash = (hash << 5) | (hash >>> 27);
            hash += (int) str.charAt(i);
        }

        return hash;
    }

    // Additive Hahsing --> Similar to polynomial but without pow(multiplier) by i
    private int customAdditiveHashCode(K key) {
        // Typecast Key into a String (since ID is a string)
        String str = key.toString();
        int hash = 0;

        // Custom Hash Functiom with set multiplier
        for (int i = 0; i < str.length(); i++) {
            hash += hashMultiplier * (int) str.charAt(i);
        }

        return hash;
    }

    private int customHashCode(K key) {
        switch (hashingStrategy) {
            case "BITWISE":
                return customBitwiseHashCode(key);
            case "POLYNOMIAL":
                return customPolynomialHashCode(key);
            case "CYCLIC":
                return customCyclicHashCode(key);
            case "ADDITIVE":
                return customAdditiveHashCode(key);
            default:
                throw new IllegalArgumentException("Unknown hashing strategy");
        }
    }

    private int findSlot(K key) {
        // Calculate the initial index using the key's hash code
        int index = Math.abs(customHashCode(key)) % table.length;
        int startIndex = index;
        int probeCount = 0;

        // Linear probing: continue until an empty slot or the key is found
        while (table[index] != null && (table[index].key == null || !table[index].key.equals(key))) {
            // Move to the next slot linearly
            index = (startIndex + ++probeCount) % table.length;
            // If we've probed all slots, the table is full
            if (index == startIndex) {
                throw new IllegalStateException("HashMap full");
            }
        }
        // Return the index where the key is found or should be inserted
        return index;
    }

    public void put(K key, V value) {
        // Check if resizing is needed before inserting
        if ((double) size / table.length > loadFactor) {
            resize();
        }

        // Find the appropriate slot for the key
        int index = findSlot(key);

        // If the slot is empty, increment the size
        if (table[index] == null || table[index].key == null) {
            size++;
        }
        // Insert or update the entry
        table[index] = new Entry<>(key, value);
    }

    public V get(K key) {
        // Find the slot where the key should be
        int index = findSlot(key);

        // If the key is found, return its value
        if (table[index] != null && table[index].key != null && table[index].key.equals(key)) {
            return table[index].value;
        }
        // Key not found
        return null;
    }

    public boolean containsKey(K key) {
        // Use the findSlot method to locate the potential position of the key
        int index = findSlot(key);

        // Check if the slot is not null and the key matches, findSlot might return an empty slot if the key is not in the map
        return table[index] != null && table[index].key != null && table[index].key.equals(key);
    }

    public V remove(K key) {
        // Use findSlot to locate the key
        int index = findSlot(key);

        if (table[index] != null && table[index].key != null && table[index].key.equals(key)) {
            V value = table[index].value;

            // Instead of removing the entry completely, we mark it as deleted by setting the key to null
            table[index].key = null;
            table[index].value = null;
            size--;
            return value;
        }
        return null;
    }

    // @SuppressWarnings("unchecked")
    private void resize() {
        // Create a new table with double the size
        Entry<K, V>[] oldTable = table;
        table = new Entry[oldTable.length * 2];
        size = 0;

        // Rehash all existing entries
        for (Entry<K, V> entry : oldTable) {
            if (entry != null && entry.key != null) {
                put(entry.key, entry.value);
            }
        }
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }
}
