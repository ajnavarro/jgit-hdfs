package com.ajnavarro.git.hadoop.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jgit.internal.storage.dfs.DfsPackDescription;
import org.eclipse.jgit.internal.storage.dfs.ReadableChannel;
import org.eclipse.jgit.lib.Ref;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RepositoryDataServiceReader {
    private SequenceFile.Reader reader;

    private final Path file;
    private final Configuration conf;
    private final ObjectMapper om;

    public RepositoryDataServiceReader(Path file, Configuration conf) throws IOException {
        this.file = file;
        this.conf = conf;
        // TODO get object mapper from a singleton
        this.om = new ObjectMapper();
    }

    public Map<String, Ref> getReferences() throws IOException {
        String referencesJson = getReader().getMetadata().get(new Text(MetadataKeys.REFERENCES)).toString();

        return this.om.readValue(referencesJson, this.om.getTypeFactory().constructMapType(ConcurrentHashMap.class, String.class, Ref.class));
    }

    public List<DfsPackDescription> getPackDescriptions() throws IOException {
        String packDescJson = getReader().getMetadata().get(new Text(MetadataKeys.PACK_DESCRIPTIONS)).toString();

        return this.om.readValue(packDescJson, this.om.getTypeFactory().constructCollectionType(List.class, DfsPackDescription.class));
    }

    private synchronized SequenceFile.Reader getReader() throws IOException {
        if (this.reader == null) {
            this.reader = new SequenceFile.Reader(this.conf,
                    SequenceFile.Reader.file(this.file)
            );
        }

        return this.reader;
    }

    public ReadableChannel getReadableChannel(String filename) throws IOException {
        Text key = new Text();
        while (getReader().next(key)) {
            if (!filename.equals(key.toString())) {
                continue;
            }
            BytesWritable value = new BytesWritable();
            getReader().getCurrentValue(value);

            // TODO instead of copy the bytearray we should use the original one
            return new ByteArrayReadableChannel(value.copyBytes(), 1024);
        }

        throw new FileNotFoundException("file " + filename + " not found");
    }

    private static class ByteArrayReadableChannel implements ReadableChannel {
        private final byte[] data;
        private final int blockSize;
        private int position;
        private boolean open = true;

        ByteArrayReadableChannel(byte[] buf, int blockSize) {
            data = buf;
            this.blockSize = blockSize;
        }

        @Override
        public int read(ByteBuffer dst) {
            int n = Math.min(dst.remaining(), data.length - position);
            if (n == 0)
                return -1;
            dst.put(data, position, n);
            position += n;
            return n;
        }

        @Override
        public void close() {
            open = false;
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public void position(long newPosition) {
            position = (int) newPosition;
        }

        @Override
        public long size() {
            return data.length;
        }

        @Override
        public int blockSize() {
            return blockSize;
        }

        @Override
        public void setReadAheadBytes(int b) {
            // Unnecessary on a byte array.
        }
    }
}
