package com.ajnavarro.git.hadoop;

import com.ajnavarro.git.hadoop.service.RepositoryDataServiceReader;
import com.ajnavarro.git.hadoop.service.RepositoryDataServiceWriter;
import org.eclipse.jgit.internal.storage.dfs.DfsObjDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsOutputStream;
import org.eclipse.jgit.internal.storage.dfs.DfsPackDescription;
import org.eclipse.jgit.internal.storage.dfs.DfsReaderOptions;
import org.eclipse.jgit.internal.storage.dfs.DfsRepository;
import org.eclipse.jgit.internal.storage.dfs.ReadableChannel;
import org.eclipse.jgit.internal.storage.pack.PackExt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DfsHdfsObjDatabase extends DfsObjDatabase {

    private final RepositoryDataServiceReader reader;
    private final RepositoryDataServiceWriter writer;

    private static final AtomicInteger packId = new AtomicInteger();

    DfsHdfsObjDatabase(DfsRepository repository, DfsReaderOptions options, RepositoryDataServiceReader reader, RepositoryDataServiceWriter writer) {
        super(repository, options);
        this.reader = reader;
        this.writer = writer;
    }

    @Override
    protected List<DfsPackDescription> listPacks() throws IOException {
        return reader.getPackDescriptions();
    }

    @Override
    protected DfsPackDescription newPack(PackSource source) {
        int id = packId.incrementAndGet();
        DfsPackDescription desc =
                new DfsPackDescription(
                        getRepository().getDescription(),
                        "pack-" + id + "-" + source.name() //$NON-NLS-1$ //$NON-NLS-2$
                );

        return desc.setPackSource(source);
    }

    @Override
    protected void commitPackImpl(
            Collection<DfsPackDescription> desc,
            Collection<DfsPackDescription> replace) throws IOException {
        List<DfsPackDescription> packs = this.reader.getPackDescriptions();

        List<DfsPackDescription> n;
        n = new ArrayList<>(desc.size() + packs.size());
        n.addAll(desc);
        n.addAll(packs);
        if (replace != null)
            n.removeAll(replace);

        this.writer.setPackDescriptions(n);
    }

    @Override
    protected void rollbackPack(Collection<DfsPackDescription> desc) {
        // Do nothing. Pack is not recorded until commitPack.
    }

    @Override
    protected ReadableChannel openFile(DfsPackDescription desc, PackExt ext) throws IOException {
        return this.reader.getReadableChannel(desc.getFileName(ext));
    }

    @Override
    protected DfsOutputStream writeFile(
            DfsPackDescription desc, final PackExt ext) throws IOException {
        this.writer.open();

        return this.writer.getOutputStream(desc.getFileName(ext));
    }
}
