package com.ajnavarro.git.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryBuilder;

import java.io.IOException;

public class DfsHdfsRepositoryBuilder extends DfsRepositoryBuilder<DfsHdfsRepositoryBuilder, DfsHdfsRepository> {
    private long blockSize = 64;
    private short replication = 3;
    private Path file;
    private Configuration conf = new Configuration(true);


    public DfsHdfsRepositoryBuilder withBlockSize(long blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    public DfsHdfsRepositoryBuilder withReplication(short replication) {
        this.replication = replication;
        return this;
    }

    public DfsHdfsRepositoryBuilder withPath(Path path) {
        this.file = path;
        return this;
    }

    public DfsHdfsRepository build() throws IOException {
        // TODO check that all params are filled
        return new DfsHdfsRepository(this, blockSize, replication, file, conf);
    }
}
