/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class CalculateAverage_rayanral {

    private static final String FILE = "./measurements.txt";

    private static final int NUM_PROCESSORS = Runtime.getRuntime().availableProcessors();

    // a little bit of bit gymnastics to parse measurement from separate bytes
    private static final int TWO_BYTE_TO_INT = 480 + 48; // 48 is the ASCII code for '0'
    private static final int THREE_BYTE_TO_INT = 4800 + 480 + 48;

    // this is quite simple class, just stores single Station
    private static final class Station {
        // this is just station name in byte array
        private final byte[] data;
        private final int hash;
        private final int length;

        private int min;
        private int max;
        private int total;
        private int count;

        private Station(byte[] data, int length, int hash, int value) {
            this.data = data;
            this.hash = hash;
            this.length = length;

            min = max = total = value;
            count = 1;
        }

        private void append(int min, int max, int total, int count) {
            if (min < this.min)
                this.min = min;
            if (max > this.max)
                this.max = max;
            this.total += total;
            this.count += count;
        }

        public void append(int value) {
            append(value, value, value, 1);
        }

        public void merge(Station other) {
            append(other.min, other.max, other.total, other.count);
        }

        @Override
        public String toString() {
            return STR."\{new String(data, 0, length, StandardCharsets.UTF_8)}=\{min / 10.0}/\{Math.round(((double) total) / count) / 10.0}/\{max / 10.0}";
        }
    }

    // list of stations we encountered in this chunk
    private static class StationList implements Iterable<Station> {

        // choose a value that _eliminates_ collisions on the test set.
        // we want to set it to something big enough to not have collisions,
        // but also we don't want memory waste, so just arbitrary huge (1B) won't work
        private final static int MAX_ENTRY = 100_000;

        private final Station[] array = new Station[MAX_ENTRY];
        private int size = 0;

        public void add(byte[] data, int stationNameLength, int stationHash, int value) {
            add(
                    stationHash, // just a hash of station name
                    () -> new Station(data, stationNameLength, stationHash, value), // create a new Station if we need one
                    existing -> existing.append(value) // if this Station already exists in StationList, we'll append its measurement
            );
        }

        public void add(Station station) {
            add(station.hash, () -> station, existing -> existing.merge(station));
        }

        private void add(int hash, Supplier<Station> create, Consumer<Station> update) {
            var position = hash % MAX_ENTRY;
            Station existing;
            // handling possible collisions by shifting position
            while ((existing = array[position]) != null && existing.hash != hash) {
                position = (position + 1) % MAX_ENTRY;
            }
            // there was no such station, create one
            if (existing == null) {
                array[position] = create.get();
                size++;
            }
            else {
                // station with such name already in StationList, merge values
                update.accept(existing);
            }
        }

        public String[] toStringArray() {
            var destination = new String[size];

            var i = 0;
            for (Station station : this)
                destination[i++] = station.toString();

            return destination;
        }

        public StationList merge(StationList other) {
            for (Station station : other)
                add(station);
            return this;
        }

        @Override
        public Iterator<Station> iterator() {
            return new Iterator<>() {
                private int index = 0;

                @Override
                public boolean hasNext() {
                    Station station = null;
                    while (index < MAX_ENTRY && (station = array[index]) == null)
                        index++;
                    return station != null;
                }

                @Override
                public Station next() {
                    if (hasNext()) {
                        return array[index++];
                    }
                    throw new NoSuchElementException();
                }
            };
        }
    }

    public static void main(String[] args) throws IOException {
        var file = new File(FILE);
        var fileSize = file.length();
        var numberOfProcessors = fileSize > 1_000_000 ? NUM_PROCESSORS : 1;
        var segmentSize = (int) Math.min(Integer.MAX_VALUE - 1, fileSize / numberOfProcessors); // bytebuffer position is an int, so can be max Integer.MAX_VALUE
        var segmentCount = (int) (fileSize / segmentSize);

        var results = IntStream.range(0, segmentCount) // take each segment
                .mapToObj(i -> parseSegment(file, fileSize, segmentSize, i)) // parse each file chunk into a separate StationList
                .reduce(StationList::merge) // merge chunks together
                .orElseGet(StationList::new) // if file was empty, we just create empty StationList
                .toStringArray();

        Arrays.sort(results, Comparator.comparing(CalculateAverage_rayanral::takeUntil));
        System.out.format("{%s}%n", String.join(", ", results));
    }

    private static StationList parseSegment(File file, long fileSize, int segmentSize, int segmentId) {
        long segmentStart = segmentId * (long) segmentSize;
        long segmentEnd = Math.min(fileSize, segmentStart + segmentSize + 100);
        try (var fileChannel = (FileChannel) Files.newByteChannel(file.toPath(), StandardOpenOption.READ)) {
            var bb = fileChannel.map(FileChannel.MapMode.READ_ONLY, segmentStart, segmentEnd - segmentStart);

            // skip to first new line
            if (segmentStart > 0) {
                while (bb.get() != '\n')
                    ;
            }

            StationList stationList = new StationList();
            while (bb.position() < segmentSize) {
                parseAndAddStation(bb, stationList);
            }
            return stationList;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void parseAndAddStation(MappedByteBuffer bb, StationList stationList) {
        // not moving this into separate method as we initialize two values simultaneously here
        // buffer is actual station name
        // and hash is ... hash
        var buffer = new byte[100];
        byte b;
        var i = 0;
        int hash = 0;
        while ((b = bb.get()) != ';') {
            // we're calculating hash and recording name at the same time
            hash = hash * 31 + b;
            buffer[i++] = b;
        }

        int measurement = getMeasurement(bb);
        stationList.add(buffer, i, Math.abs(hash), measurement);
    }

    private static int getMeasurement(MappedByteBuffer bb) {
        int value;
        byte b1 = bb.get();
        byte b2 = bb.get();
        byte b3 = bb.get();
        byte b4 = bb.get();
        if (b2 == '.') {// value is n.n
            value = (b1 * 10 + b3 - TWO_BYTE_TO_INT);
            // b4 == \n
        }
        else {
            if (b4 == '.') { // value is -nn.n
                value = -(b2 * 100 + b3 * 10 + bb.get() - THREE_BYTE_TO_INT);
            }
            else if (b1 == '-') { // value is -n.n
                value = -(b2 * 10 + b4 - TWO_BYTE_TO_INT);
            }
            else { // value is nn.n
                value = (b1 * 100 + b2 * 10 + b4 - THREE_BYTE_TO_INT);
            }
            bb.get(); // new line
        }
        return value;
    }

    private static String takeUntil(String s) {
        var pos = s.indexOf("=");
        return pos > -1 ? s.substring(0, pos) : s;
    }
}
