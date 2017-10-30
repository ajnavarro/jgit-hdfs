package com.ajnavarro.git.hadoop;

import com.ajnavarro.git.hadoop.service.RepositoryDataServiceReader;
import com.ajnavarro.git.hadoop.service.RepositoryDataServiceWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.eclipse.jgit.internal.storage.dfs.DfsObjDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsReaderOptions;
import org.eclipse.jgit.internal.storage.dfs.DfsRepository;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryBuilder;
import org.eclipse.jgit.lib.RefDatabase;

import java.io.IOException;

public class DfsHdfsRepository extends DfsRepository {
    private final DfsReaderOptions readerOptions;

    private final RepositoryDataServiceReader reader;
    private final RepositoryDataServiceWriter writer;

    public DfsHdfsRepository(DfsRepositoryBuilder builder, long blockSize, short replication, Path file, Configuration conf)
            throws IOException {
        super(builder);
        this.writer = new RepositoryDataServiceWriter(blockSize, replication, file, conf);
        this.reader = new RepositoryDataServiceReader(file, conf);
        this.readerOptions = builder.getReaderOptions();
    }

    public DfsObjDatabase getObjectDatabase() {
        return new DfsHdfsObjDatabase(this, this.readerOptions, this.reader, this.writer);
    }

    public RefDatabase getRefDatabase() {
        return new DfsHdfsRefDatabase(this, this.writer, this.reader);
    }
}
