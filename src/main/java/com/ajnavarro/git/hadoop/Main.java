package com.ajnavarro.git.hadoop;

import org.apache.hadoop.fs.Path;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.internal.storage.dfs.DfsReaderOptions;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.transport.Daemon;
import org.eclipse.jgit.transport.DaemonClient;
import org.eclipse.jgit.transport.ServiceMayNotContinueException;
import org.eclipse.jgit.transport.resolver.RepositoryResolver;
import org.eclipse.jgit.transport.resolver.ServiceNotAuthorizedException;
import org.eclipse.jgit.transport.resolver.ServiceNotEnabledException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class Main {
    private static Map<String, DfsHdfsRepository> repositories = new HashMap<>();

    final static class HdfsRepositoryResolver implements
            RepositoryResolver<DaemonClient> {
        @Override
        public Repository open(DaemonClient client, String name)
                throws RepositoryNotFoundException,
                ServiceNotAuthorizedException, ServiceNotEnabledException,
                ServiceMayNotContinueException {
            DfsHdfsRepository repo = repositories.get(name);
            if (repo == null) {
                try {
                    repo = new DfsHdfsRepositoryBuilder().withPath(new Path("testRepositories", name))
                            .withBlockSize(64)
                            .withReplication((short) 2)
                            .setReaderOptions(new DfsReaderOptions())
                            .build();
                } catch (IOException e) {
                    throw new ServiceMayNotContinueException(e);
                }
                repositories.put(name, repo);
            }

            return repo;
        }

        /**
         * Create repositories executing a standard git command: clone git://localhost/repository_name.git
         *
         * @param args
         * @throws IOException
         * @throws GitAPIException
         */
        public static void main(String[] args) throws IOException, GitAPIException {
            Daemon server = new Daemon(new InetSocketAddress(9418));
            boolean uploadsEnabled = true;
            server.getService("git-receive-pack").setEnabled(uploadsEnabled);
            server.setRepositoryResolver(new HdfsRepositoryResolver());
            server.start();
        }
    }


}