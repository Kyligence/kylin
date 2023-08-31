/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.cache.utils;

import static java.util.Objects.requireNonNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings("unchecked")
public class Hadoop3CompaUtil {

    private Hadoop3CompaUtil() {}

    public static class RemoteIterators {

        private RemoteIterators() {}

        public static <T> List<T> toList(RemoteIterator<T> source)
                throws IOException {
            List<T> l = new ArrayList<>();
            foreach(source, l::add);
            return l;
        }

        public static <T> T[] toArray(RemoteIterator<T> source) throws IOException {
            return (T[]) toList(source).toArray();
        }

        public static <T> long foreach(
                RemoteIterator<T> source,
                Consumer<? super T> consumer) throws IOException {
            long count = 0;

            try {
                while (source.hasNext()) {
                    count++;
                    consumer.accept(source.next());
                }
            } finally {
                if (source instanceof Closeable) {
                    // source is closeable, so close.
                    IOUtils.cleanupWithLogger(log, (Closeable) source);
                }
            }

            return count;
        }

        public static <T> RemoteIterator<T> remoteIteratorFromArray(T[] array) {
            return new WrappedJavaIterator<>(Arrays.stream(array).iterator());
        }

        public static <T> RemoteIterator<T> remoteIteratorFromIterator(
                Iterator<T> iterator) {
            return new WrappedJavaIterator<>(iterator);
        }
    }

    private static final class WrappedJavaIterator<T> implements RemoteIterator<T>, Closeable {

        /**
         * inner iterator..
         */
        private final Iterator<? extends T> source;

        private final Closeable sourceToClose;


        /**
         * Construct from an interator.
         * @param source source iterator.
         */
        private WrappedJavaIterator(Iterator<? extends T> source) {
            this.source = requireNonNull(source);
            sourceToClose = new MaybeClose(source);
        }

        @Override
        public boolean hasNext() {
            return source.hasNext();
        }

        @Override
        public T next() {
            return source.next();
        }

        @Override
        public String toString() {
            return "FromIterator{" + source + '}';
        }

        @Override
        public void close() throws IOException {
            sourceToClose.close();
        }
    }

    private static final class MaybeClose implements Closeable {

        private Closeable toClose;

        /**
         * Construct.
         * @param o object to close.
         */
        private MaybeClose(Object o) {
            this(o, true);
        }

        /**
         * Construct -close the object if it is closeable and close==true.
         * @param o object to close.
         * @param close should close?
         */
        private MaybeClose(Object o, boolean close) {
            if (close && o instanceof Closeable) {
                this.toClose = (Closeable) o;
            } else {
                this.toClose = null;
            }
        }

        @Override
        public void close() throws IOException {
            if (toClose != null) {
                try {
                    toClose.close();
                } finally {
                    toClose = null;
                }
            }
        }
    }

}
