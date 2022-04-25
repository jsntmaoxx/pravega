package io.pravega.segmentstore.storage.impl.chunkstream.storageos.data.cs.common;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Consumer;

public class ArrayWrapList<T> implements List<T> {
    private final T[] buffer;
    private final int offset;
    private final int capacity;

    public ArrayWrapList(T[] buffer, int offset, int capacity) {
        this.buffer = buffer;
        this.offset = offset;
        this.capacity = capacity;
    }

    @Override
    public int size() {
        return capacity - offset;
    }

    @Override
    public boolean isEmpty() {
        return capacity <= offset;
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<T> iterator() {
        return new WrapIterator();
    }

    @Override
    public T[] toArray() {
        return buffer;
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(T t) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public T get(int index) {
        assert index + offset < capacity;
        return buffer[index + offset];
    }

    @Override
    public T set(int index, T element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(int index, T element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T remove(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int indexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<T> listIterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<T> listIterator(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<T> subList(int fromIndex, int toIndex) {
        assert fromIndex <= toIndex && toIndex + offset < capacity;
        return new ArrayWrapList<>(buffer, fromIndex + offset, toIndex + offset);
    }

    private class WrapIterator implements Iterator<T> {
        private int pos;

        WrapIterator() {
            pos = offset;
        }

        @Override
        public boolean hasNext() {
            return pos != capacity;
        }

        @Override
        public T next() {
            return buffer[pos++];
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void forEachRemaining(Consumer<? super T> action) {
            throw new UnsupportedOperationException();
        }
    }
}
