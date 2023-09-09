/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.yu000hong.flume.taildirmultiline;

import com.google.common.collect.Lists;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.yu000hong.flume.taildirmultiline.TaildirSourceConfigurationConstants.BYTE_OFFSET_HEADER_KEY;

public class TailFile {
    private static final Logger logger = LoggerFactory.getLogger(TailFile.class);

    private static final byte BYTE_NL = (byte) 10;
    private static final byte BYTE_CR = (byte) 13;

    private static final int BUFFER_SIZE = 8192;
    private static final int NEED_READING = -1;

    private RandomAccessFile raf;
    private final String path;
    private final long inode;
    private long pos;
    private long lastUpdated;
    private boolean needTail;
    private final Map<String, String> headers;
    private byte[] buffer;
    private byte[] oldBuffer;
    private int bufferPos;
    private long lineReadPos;
    private LineBuffer lineBuffer;

    public TailFile(File file, Map<String, String> headers, long inode, long pos)
            throws IOException {
        this(file, headers, inode, pos, null);
    }

    public TailFile(File file, Map<String, String> headers, long inode, long pos, String prefixRegex)
            throws IOException {
        this.raf = new RandomAccessFile(file, "r");
        if (pos > 0) {
            raf.seek(pos);
            lineReadPos = pos;
        }
        this.path = file.getAbsolutePath();
        this.inode = inode;
        this.pos = pos;
        this.lastUpdated = 0L;
        this.needTail = true;
        this.headers = headers;
        this.oldBuffer = new byte[0];
        this.bufferPos = NEED_READING;
        this.lineBuffer = new LineBuffer(prefixRegex);
    }

    public RandomAccessFile getRaf() {
        return raf;
    }

    public String getPath() {
        return path;
    }

    public long getInode() {
        return inode;
    }

    public long getPos() {
        return pos;
    }

    public long getLastUpdated() {
        return lastUpdated;
    }

    public boolean needTail() {
        return needTail;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public long getLineReadPos() {
        return lineReadPos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

    public void setLastUpdated(long lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public void setNeedTail(boolean needTail) {
        this.needTail = needTail;
    }

    public void setLineReadPos(long lineReadPos) {
        this.lineReadPos = lineReadPos;
    }

    public boolean updatePos(String path, long inode, long pos) throws IOException {
        if (this.inode == inode && this.path.equals(path)) {
            setPos(pos);
            updateFilePos(pos);
            logger.info("Updated position, file: " + path + ", inode: " + inode + ", pos: " + pos);
            return true;
        }
        return false;
    }

    public void updateFilePos(long pos) throws IOException {
        raf.seek(pos);
        lineReadPos = pos;
        bufferPos = NEED_READING;
        oldBuffer = new byte[0];
        lineBuffer.reset();
    }


    public List<Event> readEvents(int numEvents, boolean backoffWithoutNL,
                                  boolean addByteOffset) throws IOException {
        List<Event> events = Lists.newLinkedList();
        for (int i = 0; i < numEvents; i++) {
            Event event = readEvent(backoffWithoutNL, addByteOffset);
            if (event == null) {
                break;
            }
            events.add(event);
        }
        return events;
    }

    private Event readEvent(boolean backoffWithoutNL, boolean addByteOffset) throws IOException {
        lineBuffer.setBackoffWithoutNL(backoffWithoutNL);
        lineBuffer.setAddByteOffset(addByteOffset);
        LineResult line;
        Event event;
        do {
            line = readLine();
            event = lineBuffer.append(line);
        } while (event == null && line != null);
        return event;
    }

    private void readFile() throws IOException {
        if ((raf.length() - raf.getFilePointer()) < BUFFER_SIZE) {
            buffer = new byte[(int) (raf.length() - raf.getFilePointer())];
        } else {
            buffer = new byte[BUFFER_SIZE];
        }
        raf.read(buffer, 0, buffer.length);
        bufferPos = 0;
    }

    private byte[] concatByteArrays(byte[] a, int startIdxA, int lenA,
                                    byte[] b, int startIdxB, int lenB) {
        byte[] c = new byte[lenA + lenB];
        System.arraycopy(a, startIdxA, c, 0, lenA);
        System.arraycopy(b, startIdxB, c, lenA, lenB);
        return c;
    }

    public LineResult readLine() throws IOException {
        LineResult lineResult = null;
        while (true) {
            if (bufferPos == NEED_READING) {
                if (raf.getFilePointer() < raf.length()) {
                    readFile();
                } else {
                    if (oldBuffer.length > 0) {
                        lineResult = new LineResult(false, oldBuffer, lineReadPos);
                        oldBuffer = new byte[0];
                        setLineReadPos(lineReadPos + lineResult.line.length);
                    }
                    break;
                }
            }
            for (int i = bufferPos; i < buffer.length; i++) {
                if (buffer[i] == BYTE_NL) {
                    int oldLen = oldBuffer.length;
                    // Don't copy last byte(NEW_LINE)
                    int lineLen = i - bufferPos;
                    // For windows, check for CR
                    if (i > 0 && buffer[i - 1] == BYTE_CR) {
                        lineLen -= 1;
                    } else if (oldBuffer.length > 0 && oldBuffer[oldBuffer.length - 1] == BYTE_CR) {
                        oldLen -= 1;
                    }
                    lineResult = new LineResult(true,
                            concatByteArrays(oldBuffer, 0, oldLen, buffer, bufferPos, lineLen), lineReadPos);
                    setLineReadPos(lineReadPos + (oldBuffer.length + (i - bufferPos + 1)));
                    oldBuffer = new byte[0];
                    if (i + 1 < buffer.length) {
                        bufferPos = i + 1;
                    } else {
                        bufferPos = NEED_READING;
                    }
                    break;
                }
            }
            if (lineResult != null) {
                break;
            }
            // NEW_LINE not showed up at the end of the buffer
            oldBuffer = concatByteArrays(oldBuffer, 0, oldBuffer.length,
                    buffer, bufferPos, buffer.length - bufferPos);
            bufferPos = NEED_READING;
        }
        return lineResult;
    }

    public void close() {
        try {
            raf.close();
            raf = null;
            long now = System.currentTimeMillis();
            setLastUpdated(now);
        } catch (IOException e) {
            logger.error("Failed closing file: " + path + ", inode: " + inode, e);
        }
    }

    private class LineResult {
        final boolean lineSepInclude;
        final byte[] line;
        final long pos;
        final String text;//just for debug

        public LineResult(boolean lineSepInclude, byte[] line, long pos) {
            super();
            this.lineSepInclude = lineSepInclude;
            this.line = line;
            this.pos = pos;
            this.text = new String(line);
        }
    }

    private class LineBuffer {
        private final List<byte[]> lines = new ArrayList<>();
        private boolean addByteOffset;
        private boolean backoffWithoutNL;
        private final Pattern prefixPattern;
        private long firstBytePos = -1;
        private boolean partial = false;

        public LineBuffer(String prefixRegex) {
            if (prefixRegex != null) {
                prefixPattern = Pattern.compile(prefixRegex);
            } else {
                prefixPattern = null;
            }
        }

        public void setAddByteOffset(boolean addByteOffset) {
            this.addByteOffset = addByteOffset;
        }

        public void setBackoffWithoutNL(boolean backoffWithoutNL) {
            this.backoffWithoutNL = backoffWithoutNL;
        }

        public void reset() {
            lines.clear();
            firstBytePos = -1;
            partial = false;
        }

        public Event append(LineResult lineResult) {
            Event event = null;
            if ((!partial && isNewRecord(lineResult))
                    || (partial && !backoffWithoutNL && lineResult == null)) {
                byte[] line = merge();
                if (line != null) {
                    event = EventBuilder.withBody(line);
                    if (addByteOffset) {
                        event.getHeaders().put(BYTE_OFFSET_HEADER_KEY, String.valueOf(firstBytePos));
                    }
                }
                lines.clear();
            }
            appendInternal(lineResult);
            return event;
        }

        private void appendInternal(LineResult lineResult) {
            if (lineResult == null) {
                return;
            }
            if (lines.isEmpty()) {
                firstBytePos = lineResult.pos;
            }
            if (partial) {
                int lastIndex = lines.size() - 1;
                byte[] last = lines.get(lastIndex);
                byte[] current = lineResult.line;
                byte[] line = new byte[last.length + current.length];
                System.arraycopy(last, 0, line, 0, last.length);
                System.arraycopy(current, 0, line, last.length, current.length);
                lines.set(lastIndex, line);
            } else {
                lines.add(lineResult.line);
            }
            partial = !lineResult.lineSepInclude;
        }

        private boolean isNewRecord(LineResult lineResult) {
            if (lineResult == null || prefixPattern == null) {
                return true;
            }
            String line = new String(lineResult.line);
            Matcher matcher = prefixPattern.matcher(line);
            return matcher.find();
        }

        private byte[] merge() {
            if (lines.isEmpty()) {
                return null;
            }
            byte[] line = lines.get(0);
            if (lines.size() > 1) {
                int len = lines.stream()
                        .map(bytes -> bytes.length + 1)
                        .reduce(0, Integer::sum) - 1;
                line = new byte[len];
                byte[] firstLine = lines.get(0);
                System.arraycopy(firstLine, 0, line, 0, firstLine.length);
                int index = firstLine.length;
                for (int i = 1; i < lines.size(); i++) {
                    line[index++] = BYTE_NL;
                    byte[] currentLine = lines.get(i);
                    System.arraycopy(currentLine, 0, line, index, currentLine.length);
                    index += currentLine.length;
                }
            }
            return line;
        }

    }

}
