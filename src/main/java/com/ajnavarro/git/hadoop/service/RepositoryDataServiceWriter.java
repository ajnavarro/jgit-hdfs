package com.ajnavarro.git.hadoop.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jgit.internal.storage.dfs.DfsOutputStream;
import org.eclipse.jgit.internal.storage.dfs.DfsPackDescription;
import org.eclipse.jgit.lib.Ref;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;

public class RepositoryDataServiceWriter {
    private final long blockSize;
    private final short replication;
    private final Path file;
    private final Configuration conf;
    private final ObjectMapper om;
    private final TreeMap<Text, Text> metadata;

    private SequenceFile.Writer writer;

    public RepositoryDataServiceWriter(long blockSize, short replication, Path file, Configuration conf) {
        this.blockSize = blockSize;
        this.replication = replication;
        this.file = file;
        this.conf = conf;

        // TODO get object mapper from a singleton
        this.om = new ObjectMapper();

        this.metadata = new TreeMap<>();
    }

    public void setReferences(ConcurrentMap<String, Ref> refs) throws IOException {
        String json = this.om.writeValueAsString(refs);
        this.metadata.put(new Text(MetadataKeys.REFERENCES), new Text(json));
    }

    public void setPackDescriptions(List<DfsPackDescription> packDesc) throws IOException {
        String json = this.om.writeValueAsString(packDesc);
        this.metadata.put(new Text(MetadataKeys.PACK_DESCRIPTIONS), new Text(json));
    }

    public void open() throws IOException {
        this.writer = SequenceFile.createWriter(this.conf,
                SequenceFile.Writer.blockSize(this.blockSize),
                SequenceFile.Writer.replication(this.replication),
                SequenceFile.Writer.file(this.file),
                SequenceFile.Writer.appendIfExists(false),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(BytesWritable.class),
                // TODO SequenceFile.Writer.progressable(Progressable),
                SequenceFile.Writer.bufferSize(1024),
                SequenceFile.Writer.metadata(new SequenceFile.Metadata(this.metadata))
        );
    }

    public DfsOutputStream getOutputStream(final String filename) {
        return new RepositoryDataServiceWriter.Out() {
            @Override
            public void flush() throws IOException {
                writer.append(new Text(filename), new BytesWritable(getData()));
            }
        };
    }

    private abstract static class Out extends DfsOutputStream {
        private final ByteArrayOutputStream dst = new ByteArrayOutputStream();

        private byte[] data;

        @Override
        public void write(byte[] buf, int off, int len) {
            data = null;
            dst.write(buf, off, len);
        }

        @Override
        public int read(long position, ByteBuffer buf) {
            byte[] d = getData();
            int n = Math.min(buf.remaining(), d.length - (int) position);
            if (n == 0)
                return -1;
            buf.put(d, (int) position, n);
            return n;
        }

        byte[] getData() {
            if (data == null)
                data = dst.toByteArray();
            return data;
        }

        @Override
        public abstract void flush() throws IOException;

        @Override
        public void close() throws IOException {
            flush();
        }
    }
}
